import matplotlib as mpl
import numpy as np
import matplotlib.pyplot as plt

X = np.linspace(0,30,50)
Y = np.sin(X)
fig,ax = plt.subplots()
ax.plot(X,Y,color="g",marker="*",linestyle="--")
plt.show()