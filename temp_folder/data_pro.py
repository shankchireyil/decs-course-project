import re
import matplotlib.pyplot as plt

# ---------------- Parsing Function ---------------- #
def parse_loadtest_results(filename):
    results = {
        "threads": [],
        "throughput": [],
        "avg_latency": [],
        "cpu_util": [],
        "disk_util": [],
        "disk_write_kb" : []
    }

    with open(filename, "r") as f:
        content = f.read()

    blocks = content.split("============================")
    
    for block in blocks:
        threads = re.search(r"threads=(\d+)", block)
        if threads:
            results["threads"].append(int(threads.group(1)))

        throughput = re.search(r"Throughput:\s+([\d.]+)\s+requests/second", block)
        if throughput:
            results["throughput"].append(float(throughput.group(1)))

        avg_latency = re.search(r"Average Latency:\s+([\d.]+)\s+ms", block)
        if avg_latency:
            results["avg_latency"].append(float(avg_latency.group(1)))

        cpu = re.search(r"Average CPU Utilisation.*?:\s+([\d.]+)\s*%", block)
        if cpu:
            results["cpu_util"].append(float(cpu.group(1)))


        write_kb = re.search(r"Average Write KB/s.*?:\s+([\d.]+)", block)
        if write_kb:
            results["disk_write_kb"].append(float(write_kb.group(1)))

        # FIXED: Disk Utilisation (%)
        disk_util = re.search(r"Average Disk Utilisation.*?:\s+([\d.]+)\s*%", block)
        if disk_util:
            results["disk_util"].append(float(disk_util.group(1)))    

    return results


# ---------------- Plot Function ---------------- #
def plot_metric(threads, values, y_name, title,metric_name):
    plt.figure()  # ensures new plot every time

    plt.plot(threads, values, marker='o', linewidth=2)
    plt.xlabel("Number of Clients (Threads)")
    plt.ylabel(y_name)
    plt.title(title)
    plt.grid(True)
    plt.xticks(threads)

    plt.savefig(f"./results_put_all/put_all_{metric_name}.png", dpi=200)
    # plt.show()


# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    filename = "load_test_results.txt"
    data = parse_loadtest_results(filename)
    print(data)  # debug

    # Loop through metrics & auto plot
    metrics_to_plot = ["throughput", "avg_latency", "cpu_util","disk_write_kb","disk_util"]
    titles = ["Throughput","Latency", "CPU Utilization", "Disk utilization","Disk Utilization"]
    titles = [f"Put-All {x}" for x in titles]
    y_axis_names = ["Throughput (req/sec)", "Latency (msec)", "CPU Utilization (%)",
              "Disk Utilization (wKB/s)","Disk Utilization (%)"]

    for i in range(len(metrics_to_plot)):
        metric = metrics_to_plot[i]
        title = titles[i]
        y_name = y_axis_names[i]
        if len(data[metric]):
            plot_metric(data["threads"], data[metric], y_name,title,metric)

