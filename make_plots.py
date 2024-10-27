import matplotlib.pyplot as plt
import numpy as np

fig, (ax0, ax1, ax2) = plt.subplots(nrows=1, ncols=3, sharex=False,sharey=False,
                                    figsize=(12, 6))

bandwidth_avgs = np.array([1.988, 2.002])
bandwidth_std = np.array([0.02049390153, 0.047644517])
ax0.errorbar(["PingAck", "PingAck+s"], bandwidth_avgs, bandwidth_std, linestyle='None', marker='^', color="black")

ax0.title.set_text('Bandwidth')
ax0.set_ylabel("Bandwidth (KiB/s)")
ax0.set_xlabel("Mode")


false_positive_rate_avgs_pingack = np.array([0.2, 4.2, 9.2, 10, 10])
false_positive_rate_std_pingack = np.array([0.4472135955, 0.4472135955, 0.4472135955, 0, 0])
ax1.errorbar(["0(%)", "1(%)", "5(%)", "10(%)", "20(%)"], false_positive_rate_avgs_pingack, false_positive_rate_std_pingack, linestyle='None', marker='^', label="PingAck")

false_positive_rate_avgs_pingacks = np.array([0, 0.4, 6.6, 10, 10])
false_positive_rate_std_pingacks = np.array([0, 0.5477225575, 0.5477225575, 0, 0])
ax1.errorbar(["0(%)", "1(%)", "5(%)", "10(%)", "20(%)"], false_positive_rate_avgs_pingacks, false_positive_rate_std_pingacks, linestyle='None', marker='^', label="PingAck+S")

ax1.title.set_text('False Positives (out of 10 VMs)')
ax1.set_ylabel("Number of incorrectly failed nodes")
ax1.set_xlabel("Drop Rate")


detection_time_avgs_pingack = np.array([2.2, 1.8, 1.8, 1.8, 1.6])
detection_time_std_pingack = np.array([0.632455532, 0.4082482905, 0.632455532, 0.9831920803, 1.471960144])
ax2.errorbar(["1", "2", "3", "4", "5"], detection_time_avgs_pingack, detection_time_std_pingack, linestyle='None', marker='^', label="PingAck")

detection_time_avgs_pingacks = np.array([6.5, 4.833333333, 4.833333333, 4.833333333, 5])
detection_time_std_pingacks = np.array([2.738612788, 1.471960144, 0.9831920803, 0.4082482905, 0])
ax2.errorbar(["1", "2", "3", "4", "5"], detection_time_avgs_pingacks, detection_time_std_pingacks, linestyle='None', marker='^', label="PingAck+S")


ax2.title.set_text('Detection time for simulataneous failures')
ax2.set_ylabel("Detection Time (s)")
ax2.set_xlabel("Number of simultaneous failures")

handles, labels = plt.gca().get_legend_handles_labels()
fig.legend(handles, labels, loc='upper right')

fig.suptitle('Worst-case detection time bound (10 seconds)')
plt.show()

