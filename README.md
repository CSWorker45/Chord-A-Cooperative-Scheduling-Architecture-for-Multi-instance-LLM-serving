# Chord
Chord is a cooperative scheduling architecture prototype to improve multi-instance LLM serving implemented on top of Llumnix (release version: 0.0.2) and vLLM (release version: 0.6.3.post1). All the additional modifications are located in the folder `llumnix_part` and `vllm_part`. <br>

## Getting Started
1. Install the backbone systems (llumnix 0.0.2, vLLM 0.6.3.post1) first. Following guidelines from https://github.com/AlibabaPAI/llumnix and https://github.com/vllm-project/vllm.
2. Insert the additional designs:
```
bash insert_Chord.sh
```

All the implementations have been integrated by the step above.

## Datasets
The sampled serving trace is contained in the folder `datasets` ready for usage.

## Serving Simulation
Use Qwen1.5-14B on ShareGPT dataset with a 4-GPU setup as an example. <br>
Start the server side as follows:
```
CUDA_VISIBLE_DEVICES="0,1,2,3" python -m llumnix.entrypoints.vllm.api_server --config-file $llumnix_dir/configs/vllm.yml --max-model-len 4096 --initial-instances 4 --model Qwen/Qwen1.5-14B-Chat --worker-use-ray --migration-backend rayrpc
```
When the server side is ready, activate the client side code to simulate the request arrivals:
```
CUDA_VISIBLE_DEVICES="0,1,2,3" python benchmark/benchmark_serving_all.py --ip_ports '127.0.0.1:1234' --tokenizer Qwen/Qwen1.5-14B-Chat --dataset_type sharegpt --qps 10.0 --distribution "gamma" --coefficient_variation 1.6 --fail_on_response_failure --allow_variable_generation_length
```
