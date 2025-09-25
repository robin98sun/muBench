import time
def cpu_profiler(i, logger = None):
    if logger:
        logger(f"CPU profiler started with input: {i}")
    start_time = time.time()
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
    
    end_time = time.time()
    output = f"{i}:{(end_time - start_time)*1000}"
    if logger:
        logger(f"CPU profiler finished with output: {output}")
    return output
