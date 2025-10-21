# Copyright (c) 2024, Alibaba Group;
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Dict, List, Tuple, Union, Iterable, Set
import math

from llumnix.logging.logger import init_logger
from llumnix.internal_config import GlobalSchedulerConfig
from llumnix.instance_info import InstanceInfo
from llumnix.global_scheduler.dispatch_scheduler import DispatchScheduler
from llumnix.global_scheduler.migration_scheduler import MigrationScheduler
from llumnix.global_scheduler.migration_policy import PairMigrationConstraints
from llumnix.global_scheduler.scaling_scheduler import ScalingScheduler
from llumnix.arg_utils import InstanceArgs

logger = init_logger(__name__)


class GlobalScheduler:
    def __init__(self, global_scheduler_config: GlobalSchedulerConfig) -> None:
        self.global_scheduler_config = global_scheduler_config
        self.num_instances = 0
        self.instance_id_set: Set[str] = set()
        self.instance_info: Dict[str, InstanceInfo] = {}

        # dispatch args
        self.dispatch_scheduler = DispatchScheduler(global_scheduler_config.dispatch_policy,
                                                    global_scheduler_config.topk_random_dispatch)
        # migrate args
        self.migration_scheduler = MigrationScheduler(global_scheduler_config.pair_migration_policy,
                                                      global_scheduler_config.migrate_out_load_threshold,
                                                      global_scheduler_config.is_group_kind_migration_backend)
        # auto-scaling args
        self.scaling_scheduler = ScalingScheduler(global_scheduler_config.scale_up_threshold,
                                                  global_scheduler_config.scale_down_threshold,
                                                  global_scheduler_config.scaling_policy,
                                                  global_scheduler_config.scaling_load_metric,
                                                  global_scheduler_config.enable_pd_disagg)

    def update_instance_infos(self, instance_infos: List[InstanceInfo]) -> None:
        for instance_info in instance_infos:
            if instance_info.instance_id in self.instance_id_set:
                self.instance_info[instance_info.instance_id] = instance_info

    def dispatch(self, req_n_blocks) -> str:
        self.dispatch_scheduler.update_instance_infos(self.instance_info)
        instance_id = self.dispatch_scheduler.dispatch(req_n_blocks)
        request_expected_steps = 1 if self.global_scheduler_config.enable_pd_disagg else math.inf
        return instance_id, request_expected_steps

    def pair_migration(self, pair_migration_type: PairMigrationConstraints) -> List[Tuple[str, str]]:
        self.migration_scheduler.update_instance_infos(self.instance_info)
        migrate_instance_pairs = self.migration_scheduler.pair_migration(pair_migration_type)
        return migrate_instance_pairs

    def get_redispatch_dst_infos(self):
        required_infos = {}
        for dst_info in self.instance_info.values():
            instance_id = dst_info.instance_id
            required_infos[instance_id] = [dst_info.num_free_gpu_blocks - dst_info.num_watermark_blocks,
                                           dst_info.num_used_gpu_blocks]
        ### logging ###
        tot_used_blocks = [getattr(instance_info, 'num_used_gpu_blocks') for instance_info in self.instance_info.values()]
        waiting_blocks = [getattr(instance_info, 'num_blocks_all_waiting_requests') for instance_info in self.instance_info.values()]
        num_waiting_requests = [getattr(instance_info, 'num_waiting_requests') for instance_info in self.instance_info.values()]
        sum_pend_time = [getattr(instance_info, 'sum_pending') for instance_info in self.instance_info.values()]
        max_pend_time = [getattr(instance_info, 'max_pending') for instance_info in self.instance_info.values()]
        avg_pend_time = [getattr(instance_info, 'sum_pending') / (getattr(instance_info, 'num_waiting_requests') + 1e-5) for instance_info in self.instance_info.values()]
        logger.info("At redispatching num waiting requests:{}, sum_pending_time:{}, max_pending_time:{}, avg_pending_time:{}".format(num_waiting_requests, sum_pend_time, max_pend_time, avg_pend_time))
        logger.info("At redispatching blocks distribution, used: {}, waiting:{}".format(tot_used_blocks, waiting_blocks))
        #########
        return required_infos
    
    def post_process(self, target_requests):
        # post process the target requests to ids for reroute_batched.
        target_request_ids = set([target_request.request_id for target_request in target_requests])
        return target_request_ids

    def derive_redispatching_plans(self, master_instance_id, global_waiting_requests, candidate_instances):
        # now we have all the pending requests in the master instance.
        # We still dispatch the requests to the instance with the original logic.
        # candidate_instances --> {instance_id: \
        # [free_blocks = total_blocks - watermark_blocks - num_used_blocks, num_used_blocks])}

        redispatch_plans = {} # {dst_instance_id:[request1, request2, ...]}
        num_redispatch = 0
        num_stay = 0

        available_instance_ids = [instance_id for instance_id, (free_blocks, _) in candidate_instances.items() if free_blocks > 0]
        if len(available_instance_ids) == 0:
            logger.info("No instance can hold any request. Stop redispatching.")
            return redispatch_plans
        else:
            for request in global_waiting_requests:
                max_used_blocks = max([candidate_instances[instance_id][1] for instance_id in available_instance_ids])
                filtered_dst_instances = [instance_id for instance_id in available_instance_ids if candidate_instances[instance_id][0] - request.n_blocks > 0]
                if len(filtered_dst_instances) == 0:
                    logger.info("No instance can hold the request {} with {} blocks. Stop redispatching.".format(request.request_id, request.n_blocks))
                    break
                else:
                    filtered_dst_instances_II = [
                        instance_id for instance_id in available_instance_ids
                        if max_used_blocks - candidate_instances[instance_id][1] - request.n_blocks > 0
                    ]
                    if len(filtered_dst_instances_II) != 0:
                        target_dst_instance_id = min(
                            filtered_dst_instances_II,
                            key=lambda inst_id: max_used_blocks - (candidate_instances[inst_id][1] + request.n_blocks)
                        )
                    else:
                        target_dst_instance_id = min(
                            filtered_dst_instances,
                            key=lambda inst_id: candidate_instances[inst_id][1] + request.n_blocks - max_used_blocks)

                    if target_dst_instance_id != master_instance_id:
                        if target_dst_instance_id not in redispatch_plans:
                            redispatch_plans[target_dst_instance_id] = []
                        redispatch_plans[target_dst_instance_id].append(request)
                        num_redispatch += 1
                    else:
                        num_stay += 1
                        # stay in the master instance, update the candidate_instances.
                    candidate_instances[target_dst_instance_id][0] -= request.n_blocks
                    candidate_instances[target_dst_instance_id][1] += request.n_blocks

                # check again
                available_instance_ids = [instance_id for instance_id, (free_blocks, _) in candidate_instances.items() if free_blocks > 0]
                if len(available_instance_ids) == 0:
                    break

        # post process to request id set.
        for dst_instance_id in redispatch_plans.keys():
            redispatch_plans[dst_instance_id] = self.post_process(redispatch_plans[dst_instance_id])

        logger.info("num_stay:{},num_redispatch:{}".format(num_stay, num_redispatch))
        return redispatch_plans

    def get_redispatch_src_instances(self):
        self.migration_scheduler.update_instance_infos(self.instance_info)
        src_instances = self.migration_scheduler.get_redispatch_src_instances()
        return src_instances

    def get_redispatch_dst_instance(self, src_instance_id, target_request):
        # instance infos have been updated in the get_redispatch_src_instance func().
        
        dst_instance = self.migration_scheduler.get_redispatch_dst_instance(src_instance_id, target_request)
        return dst_instance

    def check_scale(self) -> Tuple[str, str]:
        self.scaling_scheduler.update_instance_infos(self.instance_info)
        scale_up_num, scale_down_num = self.scaling_scheduler.check_scale()
        return scale_up_num, scale_down_num

    def scale_up(self, instance_id: Union[str, Iterable[str]], instance_args: List[InstanceArgs]) -> int:
        if isinstance(instance_id, str):
            instance_id = [instance_id,]
        instance_ids = list(instance_id)
        for ins_id, ins_args in zip(instance_ids, instance_args):
            if ins_id not in self.instance_id_set:
                logger.info("Scale up instance: {}.".format(ins_id))
                new_intance_info = self._get_empty_instance_info()
                new_intance_info.instance_id = ins_id
                self.instance_info[ins_id] = new_intance_info
                self._add_instance(ins_id, ins_args)
        logger.info("num_instances: {}, instances: {}".format(self.num_instances, self.instance_id_set))
        return self.num_instances

    def scale_down(self, instance_id: Union[str, Iterable[str]]) -> int:
        if isinstance(instance_id, str):
            instance_id = [instance_id,]
        instance_ids = list(instance_id)
        for ins_id in instance_ids:
            if ins_id in self.instance_id_set:
                logger.info("Scale down instance: {}.".format(ins_id))
                if ins_id in self.instance_info:
                    del self.instance_info[ins_id]
                else:
                    logger.warning("instance {} is not in instance_info".format(ins_id))
                self._remove_instance(ins_id)
        logger.info("num_instances: {}, instances: {}".format(self.num_instances, self.instance_id_set))
        return self.num_instances

    def _add_instance(self, instance_id: str, instance_args: InstanceArgs) -> None:
        self.instance_id_set.add(instance_id)
        self.num_instances = len(self.instance_id_set)
        for scheduler in (self.dispatch_scheduler, self.migration_scheduler, self.scaling_scheduler):
            scheduler.update_instance_infos(self.instance_info)
            scheduler.add_instance(instance_id, instance_args)

    def _remove_instance(self, instance_id: str) -> None:
        self.instance_id_set.remove(instance_id)
        self.num_instances = len(self.instance_id_set)
        for scheduler in (self.dispatch_scheduler, self.migration_scheduler, self.scaling_scheduler):
            scheduler.update_instance_infos(self.instance_info)
            scheduler.remove_instance(instance_id)

    def _get_empty_instance_info(self) -> InstanceInfo:
        return self.scaling_scheduler.get_empty_instance_info()
