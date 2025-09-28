import time

def cpu_loader_job(i):
    cpu_load = i
    pi_greco = list()
    q, r, t, k, m, x = 1, 0, 1, 1, 3, 3
    counter = 0
    while True:
        if 4 * q + r - t < m * t:
            # yield m
            pi_greco.append(str(m))
            q, r, t, k, m, x = 10*q, 10*(r-m*t), t, k, (10*(3*q+r))//t - 10*m, x
            if counter > cpu_load-1:
                break
            else:
                counter = counter+1
        else:
            q, r, t, k, m, x = q*k, (2*q+r)*x, t*x, k+1, (q*(7*k+2)+r*x)//(t*x), x+2

def select_input_values_according_to_profiling(profiling_result, latency_ms):
    # if the profiling result is sampled, return the sampled result
    if 'sampled_latencies_ms' in profiling_result and 'sampled_latencies_count' in profiling_result and profiling_result['sampled_latencies_count'] > 0:
        latency_int = int(round(latency_ms))
        if latency_int <= profiling_result['sampled_latencies_count']:

            return profiling_result['sampled_latencies_ms'][latency_int-1]

    # select the input values according to the profiling result
    statistics = profiling_result['statistics']
    remaining_latency = latency_int
    selected_input_values = []
    last_selected_index = len(statistics)-1
    while remaining_latency > 0 and last_selected_index >= 0:
        selected = False
        for i in range(last_selected_index, -1, -1):
            stat = statistics[i]
            if stat['mean'] <= remaining_latency:
                selected_input_values.append(stat['input_value'])
                remaining_latency -= stat['mean']
                last_selected_index = i
                selected = True
                break
        # if not found, it means the remaining latency is too small 
        if not selected:
            # if remaining_latency > 0:
            #     selected_input_values.append(statistics[0]['input_value'])
            #     remaining_latency -= statistics[0]['mean']
            break
    
    return selected_input_values

def cpu_payload_according_to_profiling(profiling_result, latency_ms):
    start_time = time.perf_counter()
    
    selected_input_values = select_input_values_according_to_profiling(profiling_result, latency_ms)
    
    duration_select_input_values = (time.perf_counter() - start_time) * 1000
    # select the payload according to the selected input values
    for input_value in selected_input_values:
        cpu_loader_job(input_value)
    
    duration = (time.perf_counter() - start_time) * 1000
    return duration, duration_select_input_values

def cpu_profiler(params):
    host_files = dict()
    if "_host_files_" in params:
        host_files = params["_host_files_"]
    latency = 0
    if "latency" in params:
        latency = params["latency"]
    
    profiling_result = host_files["cpu-profiling-result"]
    duration, duration_select_input_values = cpu_payload_according_to_profiling(profiling_result, latency)
    
    output = f"{latency}|{duration}|{duration_select_input_values}"

    return output
