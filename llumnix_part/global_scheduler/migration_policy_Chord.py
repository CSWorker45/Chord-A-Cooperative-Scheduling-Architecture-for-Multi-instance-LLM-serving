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

from typing import List, Tuple
from abc import ABC, abstractmethod
from enum import Enum
import numpy as np

from llumnix.logging.logger import init_logger
from llumnix.instance_info import InstanceInfo

logger = init_logger(__name__)


class PairMigrationConstraints(str, Enum):
    """Target of Migration."""
    NO_CONSTRAINTS = "NO_CONSTRAINTS"
    PREFILL_REROUTING = "PREFILL_REROUTING"
    # Enable the prefill-decode disaggregration.
    DECODE_2_DECODE = "DECODE_2_DECODE"
    PREFILL_2_DECODE = "PREFILL_2_DECODE"


class PairMigrationPolicy(ABC):
    def __init__(self, migrate_out_load_threshold: float) -> None:
        self.migrate_out_load_threshold = migrate_out_load_threshold

    @abstractmethod
    def pair_migration(self,
                       src_instance_infos: List[InstanceInfo],
                       dst_instance_infos: List[InstanceInfo],
                       ) -> List[Tuple[str, str]]:
        raise NotImplementedError

    def sort_instance_infos(self, instance_infos: List[InstanceInfo], descending: bool = True) -> None:
        key_attr = 'migration_load_metric'
        sorted_instance_infos = sorted(
            instance_infos,
            key=lambda instance_info: getattr(instance_info, key_attr),
            reverse=descending
        )
        return sorted_instance_infos

class Urgency(PairMigrationPolicy):
    def pair_migration(self,
                       src_instance_infos: List[InstanceInfo],
                       dst_instance_infos: List[InstanceInfo],
                       ) -> List[Tuple[str, str]]:
        return

    def get_src_instances(self, src_instance_infos):
        filtered_src_instance_infos = list(filter(lambda x: x.num_waiting_requests > 0, src_instance_infos))
        if len(filtered_src_instance_infos) != 0:
            sorted_src_instance_infos = sorted(src_instance_infos, key=lambda instance_info: getattr(instance_info, 'num_waiting_requests'),
                                                 reverse=True)
            target_src = [x.instance_id for x in sorted_src_instance_infos]
        else:
            target_src = None

        return target_src
    
    def get_dst_instance(self, dst_instance_infos, src_instance_id, target_request):
        req_n_blocks = target_request.n_blocks

        tot_used_blocks = [getattr(instance_info, 'num_used_gpu_blocks') for instance_info in dst_instance_infos]
        waiting_blocks = [getattr(instance_info, 'num_blocks_all_waiting_requests') for instance_info in dst_instance_infos]
        num_waiting_requests = [getattr(instance_info, 'num_waiting_requests') for instance_info in dst_instance_infos]
        sum_pend_time = [getattr(instance_info, 'sum_pending') for instance_info in dst_instance_infos]
        max_pend_time = [getattr(instance_info, 'max_pending') for instance_info in dst_instance_infos]
        avg_pend_time = [getattr(instance_info, 'sum_pending') / (getattr(instance_info, 'num_waiting_requests') + 1e-5) for instance_info in dst_instance_infos]

        filtered_dst_instance_infos = list(filter(lambda x: x.num_free_gpu_blocks - x.num_watermark_blocks - req_n_blocks > 0, dst_instance_infos))
        if len(filtered_dst_instance_infos) != 0:
            sorted_instance_infos = sorted(filtered_dst_instance_infos, key=lambda x: x.num_free_gpu_blocks / (x.num_running_requests + 1e-5), reverse=True)
            target_dst = sorted_instance_infos[0].instance_id
            if target_dst == src_instance_id:
                logger.info("Try redispatching, but current instance is the best choice, so no redispatching")
                target_dst = None
            else:
                logger.info("At redispatching waiting requests:{}, sum_pending_time:{}, max_pending_time:{}, avg_pending_time:{}".format(num_waiting_requests, sum_pend_time, max_pend_time, avg_pend_time))
                logger.info("At redispatching blocks distribution, used: {}, waiting:{}".format(tot_used_blocks, waiting_blocks))
                logger.info("Redispatching success. Target request block:{}, free space in new dst:{}".format(req_n_blocks, sorted_instance_infos[0].num_free_gpu_blocks))
        else:
            logger.info("Try redispatching, but no instance is available, so no redispatching")
            target_dst = None
        return target_dst
        

class Balanced(PairMigrationPolicy):
    def pair_migration(self,
                       src_instance_infos: List[InstanceInfo],
                       dst_instance_infos: List[InstanceInfo],
                       ) -> List[Tuple[str, str]]:
        sorted_src_instance_infos = self.sort_instance_infos(src_instance_infos, descending=True)
        sorted_dst_instance_infos = self.sort_instance_infos(dst_instance_infos, descending=False)
        migrate_instance_pairs = []
        for i in range(min(len(sorted_src_instance_infos), len(sorted_dst_instance_infos))):
            load_diff_before_mig = sorted_src_instance_infos[i].migration_load_metric - sorted_dst_instance_infos[i].migration_load_metric
            left_load_after_mig = sorted_src_instance_infos[i].migration_load_metric_after_migrate_out
            right_load_after_mig = sorted_dst_instance_infos[i].migration_load_metric_after_migrate_in
            # Add some constrains to reduce unnecessary migrations
            if right_load_after_mig > self.migrate_out_load_threshold:
                continue
            load_diff_after_mig = left_load_after_mig - right_load_after_mig
            if (0 < load_diff_after_mig < load_diff_before_mig) or (sorted_dst_instance_infos[i].migration_load_metric == -np.inf):
                migrate_instance_pairs.append((sorted_src_instance_infos[i].instance_id,
                                               sorted_dst_instance_infos[i].instance_id))
        return migrate_instance_pairs


class Defrag(PairMigrationPolicy):
    def pair_migration(self,
                       src_instance_infos: List[InstanceInfo],
                       dst_instance_infos: List[InstanceInfo],
                       ) -> List[Tuple[str, str]]:
        sorted_src_instance_infos = self.sort_instance_infos(src_instance_infos, descending=True)
        sorted_dst_instance_infos = self.sort_instance_infos(dst_instance_infos, descending=False)
        migrate_instance_pairs = []
        for i in range(min(len(sorted_src_instance_infos), len(sorted_dst_instance_infos))):
            migrate_instance_pairs.append((sorted_src_instance_infos[i].instance_id, sorted_dst_instance_infos[i].instance_id))
        return migrate_instance_pairs


class PairMigrationPolicyFactory:
    _POLICY_REGISTRY = {
        'balanced': Balanced,
        'defrag': Defrag,
        'urgency': Urgency,
    }

    @classmethod
    def get_policy(cls, policy_name: str, **kwargs) -> PairMigrationPolicy:
        return cls._POLICY_REGISTRY[policy_name](**kwargs)
