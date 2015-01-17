import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as stats

N = 1000
steps = np.random.gamma(8, 1, N) * 1000
x = [int(step) for step in steps]
y = [(np.random.gamma(180, 1, 1)[0] - step / 250) for step in x]

m, b, r, p, err = stats.linregress(x, y)

rSq = r**2

fig, ax = plt.subplots()

ax.annotate('$y=%(m).3fx+%(b).2f$\n$r^2=%(rSq).4f$' % {"m": m, "b": b, "rSq": rSq},
            xy=(0, 0), xycoords='axes fraction', fontsize=18, ha='left', va='bottom')

colors = np.random.rand(N)
plt.scatter(x, y, 100, c=colors, alpha=0.5)

lineY = [m * val + b for val in x]

plt.plot(x, lineY, linewidth=4)

plt.suptitle("Impact of Physical Activity on Blood Sugar", fontsize=20)
plt.xlabel("Steps/Day (src: fitbit)", fontsize=18)
plt.ylabel("Average Blood Sugar (src: dexcom)", fontsize=18)

plt.show()
