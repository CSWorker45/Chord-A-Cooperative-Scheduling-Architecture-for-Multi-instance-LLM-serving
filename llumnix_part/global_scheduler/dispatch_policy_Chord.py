from typing import Dict, List
from abc import ABC, abstractmethod
import random

from llumnix.logging.logger import init_logger
from llumnix.instance_info import InstanceInfo


logger = init_logger(__name__)


def sort_instance_infos(available_instance_infos: List[InstanceInfo],
                        key_attr: str,
                        descending: bool = False) -> None:
    return sorted(
        available_instance_infos,
        key=lambda instance_info: getattr(instance_info, key_attr),
        reverse=descending
    )

def filter_src_redispatch_instances(available_instance_infos: List[InstanceInfo],
                             redispatch_threshold: float) -> List[InstanceInfo]: # policy: max_pending time (experimental)
        # Filter out instances that are not eligible for redispatching
        condition = lambda instance_info: instance_info.max_pending_time >= redispatch_threshold
        # Step 3: Filter numbers that satisfy the condition
        filtered_src_instance_infos = [info for info in instance_infos if condition[info]]
        return filtered_src_instance_infos

def random_choice_from_top_k(sorted_instance_infos: List[InstanceInfo],
                             topk_random_dispatch: int):
    k = min(topk_random_dispatch, len(sorted_instance_infos))
    top_k_instance_infos = sorted_instance_infos[:k]
    return random.choice(top_k_instance_infos)


class DispatchPolicy(ABC):
    @abstractmethod
    def dispatch(self,
                 instance_num_requests: Dict[str, int],
                 available_instance_infos: List[InstanceInfo],
                 topk_random_dispatch: int,
                 req_n_blocks = None) -> int:
        pass


# Dispatch all requests to a single instance, used only for testing
class Flood(DispatchPolicy):
    def dispatch(self,
                 instance_num_requests: Dict[str, int],
                 available_instance_infos: List[InstanceInfo],
                 topk_random_dispatch: int,
                 req_n_blocks = None) -> str:
        instance_id = max(instance_num_requests, key=instance_num_requests.get)
        return instance_id

class Loadv2(DispatchPolicy):
    def dispatch(self,
                 instance_num_requests: Dict[str, int],
                 available_instance_infos: List[InstanceInfo],
                 topk_random_dispatch: int,
                 req_n_blocks = None) -> str:
        # tot_blocks = available_instance_infos[0].num_total_gpu_blocks
        tot_used_blocks = [getattr(instance_info, 'num_used_gpu_blocks') for instance_info in available_instance_infos]
        waiting_blocks = [getattr(instance_info, 'num_blocks_all_waiting_requests') for instance_info in available_instance_infos] 
        num_waiting_requests = [getattr(instance_info, 'num_waiting_requests') for instance_info in available_instance_infos]
        sum_pend_time = [getattr(instance_info, 'sum_pending') for instance_info in available_instance_infos]
        max_pend_time = [getattr(instance_info, 'max_pending') for instance_info in available_instance_infos]
        avg_pend_time = [getattr(instance_info, 'sum_pending') / (getattr(instance_info, 'num_waiting_requests') + 1e-5) for instance_info in available_instance_infos]
        logger.info("At dispatching waiting requests:{}, sum_pending_time:{}, max_pending_time:{}, avg_pending_time:{}".format(num_waiting_requests, sum_pend_time, max_pend_time, avg_pend_time))
        logger.info("At dispatching blocks distribution, used: {}, waiting:{}".format(tot_used_blocks, waiting_blocks))
        max_used_blocks = max(tot_used_blocks)

        filtered_dst_instance_infos = list(filter(lambda x: x.num_waiting_requests == 0, available_instance_infos))
        
        if len(filtered_dst_instance_infos) == len(available_instance_infos):
            # there are no waiting requests, system is not overloaded
            filtered_dst_instance_infos_II = list(filter(lambda x: max_used_blocks - x.num_watermark_blocks - x.num_used_gpu_blocks - \
                                                   req_n_blocks >= 0, available_instance_infos))
            if len(filtered_dst_instance_infos_II) > 0:
                sorted_instance_infos = sorted(filtered_dst_instance_infos_II, key = lambda x: max_used_blocks - x.num_watermark_blocks - x.num_used_gpu_blocks - req_n_blocks)
                logger.info("System is not overloaded at dispatching.")
            else:
                sorted_instance_infos = sorted(available_instance_infos, key = lambda x: x.num_used_gpu_blocks + req_n_blocks + x.num_watermark_blocks - max_used_blocks)
                logger.info("The system is growing steadily.")
        else:
            # there are waiting requests, system is overloaded
            #### there is a bug here. filtered_dst_instance_infos contain the instances that have no waiting requests.####
            sorted_instance_infos = sorted(available_instance_infos, key = \
                                           lambda x: x.num_waiting_requests, reverse=True)
            logger.info("System is overloaded. Dispatch to the logical scheduling center.")
        instance_id = sorted_instance_infos[0].instance_id

        return instance_id

class Balanced(DispatchPolicy):
    def dispatch(self,
                 instance_num_requests: Dict[str, int],
                 available_instance_infos: List[InstanceInfo],
                 topk_random_dispatch: int,
                 req_n_blocks = None) -> str:
        # dispatch request according to the number of requests dispatched to instance by manager
        instance_id = min(instance_num_requests, key=instance_num_requests.get)
        return instance_id


class Load(DispatchPolicy):
    def dispatch(self,
                 instance_num_requests: Dict[str, int],
                 available_instance_infos: List[InstanceInfo],
                 topk_random_dispatch: int, 
                 req_n_blocks = None) -> str:
        sorted_instance_infos = sort_instance_infos(available_instance_infos, 'dispatch_load_metric')
        instance_info_chosen = random_choice_from_top_k(sorted_instance_infos, topk_random_dispatch)
        instance_id = instance_info_chosen.instance_id
        logger.info("dispatch to {}, load: {}".format(instance_id, instance_info_chosen.dispatch_load_metric))
        return instance_id
    

class Queue(DispatchPolicy):
    def dispatch(self,
                 instance_num_requests: Dict[str, int],
                 available_instance_infos: List[InstanceInfo],
                 topk_random_dispatch: int,
                 req_n_blocks = None) -> str:
        sorted_instance_infos = sort_instance_infos(available_instance_infos, 'num_waiting_requests')
        instance_info_chosen = random_choice_from_top_k(sorted_instance_infos, topk_random_dispatch)
        instance_id = instance_info_chosen.instance_id
        logger.info("dispatch to {}, queue size: {}".format(instance_id, instance_info_chosen.num_waiting_requests))
        return instance_id


class RoundRobin(DispatchPolicy):
    prev_instance_idx: int = -1

    def dispatch(self,
                 instance_num_requests: Dict[str, int],
                 available_instance_infos: List[InstanceInfo],
                 topk_random_dispatch: int,
                 req_n_blocks = None) -> str:
        all_instance_ids = sorted(instance_num_requests.keys())
        cur_instance_idx = (self.prev_instance_idx + 1) % len(all_instance_ids)
        target_instance_id = all_instance_ids[cur_instance_idx]
        self.prev_instance_idx = cur_instance_idx
        return target_instance_id


class DispatchPolicyFactory:
    _POLICY_REGISTRY = {
        'flood': Flood,
        'balanced': Balanced,
        'load': Load,
        'queue': Queue,
        'rr': RoundRobin,
        'loadv2': Loadv2,
    }

    @classmethod
    def get_policy(cls, policy_name: str, **kwargs) -> DispatchPolicy:
        return cls._POLICY_REGISTRY[policy_name](**kwargs)
