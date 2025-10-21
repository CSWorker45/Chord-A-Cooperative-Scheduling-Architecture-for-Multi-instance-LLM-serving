#!/bin/bash

# Get the vllm package source directory using Python
vllm_dir=$(python3 -c "import importlib.util; spec = importlib.util.find_spec('vllm'); print(spec.origin.rsplit('/', 1)[0])")

# Check if the directory was successfully located
if [ -z "$vllm_dir" ]; then
  echo "vllm package not found. Exiting."
  exit 1
fi

echo "vllm package found at: $vllm_dir"

llumnix_dir=$(python3 -c "import importlib.util; spec = importlib.util.find_spec('llumnix'); print(spec.origin.rsplit('/', 1)[0])")

# Check if the directory was successfully located
if [ -z "$llumnix_dir" ]; then
  echo "llumnix package not found. Exiting."
  exit 1
fi

echo "llumnix package found at: $llumnix_dir"

# Replace files in vllm package
cp "./vllm_part/arg_utils_Chord.py" "$vllm_dir/engine/arg_utils.py"
cp "./vllm_part/config_Chord.py" "$vllm_dir/config.py"
cp "./vllm_part/scheduler_Chord.py" "$vllm_dir/core/scheduler.py"

# Replace files in llumnix package
cp "./llumnix_part/manager_Chord.py" "$llumnix_dir/llumnix/manager.py"
cp "./llumnix_part/instance_info_Chord.py" "$llumnix_dir/llumnix/instance_info.py"
cp "./llumnix_part/config/default_Chord.py" "$llumnix_dir/llumnix/config/default.py"

# global schedulers
cp "./llumnix_part/global_scheduler/global_scheduler_Chord.py" "$llumnix_dir/llumnix/global_scheduler/global_scheduler.py"
cp "./llumnix_part/global_scheduler/dispatch_scheduler_Chord.py" "$llumnix_dir/llumnix/global_scheduler/dispatch_scheduler.py"
cp "./llumnix_part/global_scheduler/dispatch_policy_Chord.py" "$llumnix_dir/llumnix/global_scheduler/dispatch_policy.py"
cp "./llumnix_part/global_scheduler/migration_filter_Chord.py" "$llumnix_dir/llumnix/global_scheduler/migration_filter.py"
cp "./llumnix_part/global_scheduler/migration_policy_Chord.py" "$llumnix_dir/llumnix/global_scheduler/migration_policy.py"
cp "./llumnix_part/global_scheduler/migration_scheduler_Chord.py" "$llumnix_dir/llumnix/global_scheduler/migration_scheduler.py"

# local instances
cp "./llumnix_part/llumlet/llumlet_Chord.py" "$llumnix_dir/llumnix/llumlet/llumlet.py"
cp "./llumnix_part/llumlet/local_migration_scheduler_Chord.py" "$llumnix_dir/llumnix/llumlet/local_migration_scheduler.py"
cp "./llumnix_part/llumlet/migration_coordinator_Chord.py" "$llumnix_dir/llumnix/llumlet/migration_coordinator.py"

cp "./llumnix_part/backends/vllm/llm_engine_Chord.py" "$llumnix_dir/llumnix/backends/vllm/llm_engine.py"
cp "./llumnix_part/backends/vllm/scheduler_Chord.py" "$llumnix_dir/llumnix/backends/vllm/scheduler.py"
cp "./llumnix_part/backends/vllm/sequence_Chord.py" "$llumnix_dir/llumnix/backends/vllm/sequence.py"
