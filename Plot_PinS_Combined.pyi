import numpy as np
import math
import matplotlib.pyplot as plt

# Fixed Parameters
PinS_to_FHP = 800 # m
FHP_to_GARP = 3000 # m

# Flexible Parameters
GP = 3.7 # deg  (maximum 6.3 deg, optimum 3.7 deg)
FHPCH = 15 # m (typically 15 m, increase FHPCH as necessary)

MA_Climb_Gradient = 0.042 #

OCH_PinS = PinS_to_FHP * math.tan(GP * math.pi / 180) + FHPCH # ft (minimum 250 ft)

x_origin_of_Z_Surface = -(700 + 38 / math.tan(GP * math.pi / 180))

print("x_origin_of_Z_Surface :" + str(x_origin_of_Z_Surface)+ ' m')

x_PinS = 1852

print("OCH_PinS :" + str(OCH_PinS)+ ' m')
print("OCH_PinS :" + str(OCH_PinS * 3.2808) + ' ft')

PinS_to_HRP = 1 * 1852 # m

Safety_AW = 15 # m
HCH = 3 # m
alpha = 15 # deg (night time operation)

ATT = 444 # m

O1 = [2700, 400, 17] # O1 (x, y, z)
O2 = [4000, 1350, 81] # O2 (x, y, z)
O3 = [500, 40, 19] # O3
O4 = [1000, 80, 30] # O4
O5 = [200, 150, 65] # O5
O6 = [-700, 300, 70] # O6

plt.figure(1)

W_Surface = [0.03494, 0, -6.45]
W2_Surface = [0.0485, 0, -38.77]
X_Surface = [0.032556, 0.2138, -58.89]
Y_Surface = [0.025341, 0.26772, -67.93]
Z_Surface = [-MA_Climb_Gradient, 0, -58.29]

# Foot Print Contour
C = [799.18 + 1052, 153.76]
D = [375.1 + 1052, 218.18, 218.14]
E = [-1387.63 + 1052, 485.75]

C1 = [8770.75 + 1052, 344, 300]
C2 = [2382.75 + 1052, 271.88, 74.81]
D1 = [5281.05 + 1052, 874.47, 300]
E1 = [-5747.08 + 1052, 1481.6, 183.1]

plt.plot(np.linspace(-C2[0], -C2[0], 1000),np.linspace(C2[1], -C2[1], 1000),'r', linewidth=0.7)

plt.plot(np.linspace(-E[0], -E[0], 1000),np.linspace(E[1], -E[1], 1000),'r', linewidth=0.7)

plt.plot(np.linspace(-E1[0], -E1[0], 1000),np.linspace(E1[1], -E1[1], 1000),'r', linewidth=0.7)

plt.plot(np.linspace(-C[0], -C[0], 1000),np.linspace(C[1], -C[1], 1000),'r', linewidth=0.7)
plt.plot(np.linspace(-C1[0], -C1[0], 1000),np.linspace(C1[1], -C1[1], 1000),'r', linewidth=0.7)

plt.plot(np.linspace(-C[0], -D[0], 1000),np.linspace(C[1], D[1], 1000),'r', linewidth=0.7)
plt.plot(np.linspace(-C[0], -D[0], 1000),np.linspace(-C[1], -D[1], 1000),'r', linewidth=0.7)

plt.plot(np.linspace(-E[0], -D[0], 1000),np.linspace(E[1], D[1], 1000),'r', linewidth=0.7)
plt.plot(np.linspace(-E[0], -D[0], 1000),np.linspace(-E[1], -D[1], 1000),'r', linewidth=0.7)

plt.plot(np.linspace(-D[0], -D1[0], 1000),np.linspace(D[1], D1[1], 1000),'r', linewidth=0.7)
plt.plot(np.linspace(-D[0], -D1[0], 1000),np.linspace(-D[1], -D1[1], 1000),'r', linewidth=0.7)

plt.plot(np.linspace(-C1[0], -D1[0], 1000),np.linspace(C1[1], D1[1], 1000),'r', linewidth=0.7)
plt.plot(np.linspace(-C1[0], -D1[0], 1000),np.linspace(-C1[1], -D1[1], 1000),'r', linewidth=0.7)

plt.plot(np.linspace(-E1[0], -E[0], 1000),np.linspace(E1[1], E[1], 1000),'r', linewidth=0.7)
plt.plot(np.linspace(-E1[0], -E[0], 1000),np.linspace(-E1[1], -E[1], 1000),'r', linewidth=0.7)

plt.plot(np.linspace(-C2[0], -C[0], 1000),np.linspace(C2[1], C[1], 1000),'r', linewidth=0.7)
plt.plot(np.linspace(-C2[0], -C[0], 1000),np.linspace(-C2[1], -C[1], 1000),'r', linewidth=0.7)

plt.plot(np.linspace(-C1[0], -C2[0], 1000),np.linspace(C1[1], C2[1], 1000),'r', linewidth=0.7)
plt.plot(np.linspace(-C1[0], -C2[0], 1000),np.linspace(-C1[1], -C2[1], 1000),'r', linewidth=0.7)

plt.plot(np.linspace(-E1[0], -10000, 1000),np.linspace(0.8 * 1852, 0.8 * 1852, 1000),'r', linewidth=0.7)
plt.plot(np.linspace(-E1[0], -10000, 1000),np.linspace(-0.8 * 1852, -0.8 * 1852, 1000),'r', linewidth=0.7)

plt.scatter(-O1[0]-PinS_to_HRP,O1[1])
plt.annotate('O1', (-O1[0]-PinS_to_HRP,O1[1]))

plt.scatter(-O2[0]-PinS_to_HRP,O2[1])
plt.annotate('O2', (-O2[0]-PinS_to_HRP,O2[1]))

plt.scatter(-O3[0],O3[1],s = 20)
plt.annotate('O3', (-O3[0] + 20,O3[1] + 20))

plt.scatter(-O4[0],O4[1],s = 20)
plt.annotate('O4', (-O4[0] + 20,O4[1] + 20))

plt.scatter(-O5[0],O5[1],s = 20)
plt.annotate('O5', (-O5[0] + 20,O5[1] + 20))

plt.scatter(-O6[0],O6[1],s = 20)
plt.annotate('O6', (-O6[0] + 20,O6[1] + 20))

plt.scatter(0,0,s=20, c='k')
plt.annotate('HRP', (0+20,0 - 200))

plt.scatter(-x_PinS,0,s=20, c='k')
plt.annotate('MAPt', (-x_PinS+20 , 0 - 200))

plt.scatter(-x_PinS + PinS_to_FHP, 0 ,s=20, c='k')
plt.annotate('FHP', (-x_PinS + PinS_to_FHP +20, 0 - 200))

plt.grid(color='k', linestyle='-', linewidth=0.1)
plt.axis('equal')

# ============ Figure 2 =================== #

# plt.figure(2)

# Fixed Parameters
PinS_to_FHP = 800 # m
FHP_to_GARP = 3000 # m

# Flexible Parameters
GP = 3.7 # deg  (maximum 6.3 deg, optimum 3.7 deg)
FHPCH = 15 # m (typically 15 m, increase FHPCH as necessary)

MA_Climb_Gradient = 0.042 #

OCH_PinS = PinS_to_FHP * math.tan(GP * math.pi / 180) + FHPCH # ft (minimum 250 ft)

x_origin_of_Z_Surface = -(700 + 38 / math.tan(GP * math.pi / 180))

print("x_origin_of_Z_Surface :" + str(x_origin_of_Z_Surface)+ ' m')

x_PinS = 1852

print("OCH_PinS :" + str(OCH_PinS)+ ' m')
print("OCH_PinS :" + str(OCH_PinS * 3.2808) + ' ft')

PinS_to_HRP = 1 * 1852 # m

Safety_AW = 15 # m
HCH = 3 # m
alpha = 15 # deg (night time operation)

ATT = 444 # m

O1 = [2700, 400, 17] # O1 (x, y, z)
O2 = [4000, 1350, 81] # O2 (x, y, z)
O3 = [500, 40, 19] # O3
O4 = [1000, 80, 30] # O4
O5 = [200, 150, 65] # O5
O6 = [-700, 300, 70] # O6


W_Surface = [0.05012, 0, -6.45]
W2_Surface = [0.0701, 0, -38.77]
X_Surface = [0.032556, 0.2138, -58.89]
Y_Surface = [0.025341, 0.26772, -67.93]
Z_Surface = [-MA_Climb_Gradient, 0, -58.29]

# Foot Print Contour
C = [553.07 + 1052, 153.76]
D = [375.1 + 1052, 218.18, 218.14]
E = [-2030.78 + 1052, 485.75]

C1 = [6114.33 + 1052, 748, 300]
C2 = [1617.62 + 1052, 378.19, 74.81]
D1 = [5281.05 + 1052, 874.47, 300]
E1 = [-6148.18 + 1052, 1481.6, 183.1]

plt.plot(np.linspace(-C2[0], -C2[0], 1000),np.linspace(C2[1], -C2[1], 1000),'k', linewidth=0.7)

plt.plot(np.linspace(-E[0], -E[0], 1000),np.linspace(E[1], -E[1], 1000),'k', linewidth=0.7)

plt.plot(np.linspace(-E1[0], -E1[0], 1000),np.linspace(E1[1], -E1[1], 1000),'k', linewidth=0.7)

plt.plot(np.linspace(-C[0], -C[0], 1000),np.linspace(C[1], -C[1], 1000),'k', linewidth=0.7)
plt.plot(np.linspace(-C1[0], -C1[0], 1000),np.linspace(C1[1], -C1[1], 1000),'k', linewidth=0.7)

plt.plot(np.linspace(-C[0], -D[0], 1000),np.linspace(C[1], D[1], 1000),'k', linewidth=0.7)
plt.plot(np.linspace(-C[0], -D[0], 1000),np.linspace(-C[1], -D[1], 1000),'k', linewidth=0.7)

plt.plot(np.linspace(-E[0], -D[0], 1000),np.linspace(E[1], D[1], 1000),'k', linewidth=0.7)
plt.plot(np.linspace(-E[0], -D[0], 1000),np.linspace(-E[1], -D[1], 1000),'k', linewidth=0.7)

plt.plot(np.linspace(-D[0], -D1[0], 1000),np.linspace(D[1], D1[1], 1000),'k', linewidth=0.7)
plt.plot(np.linspace(-D[0], -D1[0], 1000),np.linspace(-D[1], -D1[1], 1000),'k', linewidth=0.7)

plt.plot(np.linspace(-C1[0], -D1[0], 1000),np.linspace(C1[1], D1[1], 1000),'k', linewidth=0.7)
plt.plot(np.linspace(-C1[0], -D1[0], 1000),np.linspace(-C1[1], -D1[1], 1000),'k', linewidth=0.7)

plt.plot(np.linspace(-E1[0], -E[0], 1000),np.linspace(E1[1], E[1], 1000),'k', linewidth=0.7)
plt.plot(np.linspace(-E1[0], -E[0], 1000),np.linspace(-E1[1], -E[1], 1000),'k', linewidth=0.7)

plt.plot(np.linspace(-C2[0], -C[0], 1000),np.linspace(C2[1], C[1], 1000),'k', linewidth=0.7)
plt.plot(np.linspace(-C2[0], -C[0], 1000),np.linspace(-C2[1], -C[1], 1000),'k', linewidth=0.7)

plt.plot(np.linspace(-C1[0], -C2[0], 1000),np.linspace(C1[1], C2[1], 1000),'k', linewidth=0.7)
plt.plot(np.linspace(-C1[0], -C2[0], 1000),np.linspace(-C1[1], -C2[1], 1000),'k', linewidth=0.7)

plt.plot(np.linspace(-E1[0], -10000, 1000),np.linspace(0.8 * 1852, 0.8 * 1852, 1000),'k', linewidth=0.7)
plt.plot(np.linspace(-E1[0], -10000, 1000),np.linspace(-0.8 * 1852, -0.8 * 1852, 1000),'k', linewidth=0.7)

plt.scatter(-O1[0]-PinS_to_HRP,O1[1])
plt.annotate('O1', (-O1[0]-PinS_to_HRP,O1[1]))

plt.scatter(-O2[0]-PinS_to_HRP,O2[1])
plt.annotate('O2', (-O2[0]-PinS_to_HRP,O2[1]))

plt.scatter(-O3[0],O3[1],s = 20)
plt.annotate('O3', (-O3[0] + 20,O3[1] + 20))

plt.scatter(-O4[0],O4[1],s = 20)
plt.annotate('O4', (-O4[0] + 20,O4[1] + 20))

plt.scatter(-O5[0],O5[1],s = 20)
plt.annotate('O5', (-O5[0] + 20,O5[1] + 20))

plt.scatter(-O6[0],O6[1],s = 20)
plt.annotate('O6', (-O6[0] + 20,O6[1] + 20))

plt.scatter(0,0,s=20, c='k')
plt.annotate('HRP', (0+20,0 - 200))

plt.scatter(-x_PinS,0,s=20, c='k')
plt.annotate('MAPt', (-x_PinS+20 , 0 - 200))

plt.scatter(-x_PinS + PinS_to_FHP, 0 ,s=20, c='k')
plt.annotate('FHP', (-x_PinS + PinS_to_FHP +20, 0 - 200))

plt.grid(color='k', linestyle='-', linewidth=0.1)
plt.axis('equal')

plt.show()



