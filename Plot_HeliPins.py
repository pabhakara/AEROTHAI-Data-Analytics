import numpy as np
import math
import matplotlib.pyplot as plt
import subprocess
import shlex
import shapely as shp

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

MOC_final = 75 # m
MOC_missed_approach = 30 # m

OCA_O1 = math.ceil(O1[2] + MOC_final)
OCA_O2 = math.ceil(O2[2] + MOC_final * ((0.4 * 1852) - (1350 - 0.4 * 1852)) / (0.4 * 1852))

OCS_level = OCA_O2 + HCH - MOC_final# m

OCA_O4 = math.ceil(O4[2] + MOC_final)

OCH = OCA_O4

print("==== Approach ======")

print("Level OCS: " + str(OCS_level) + ' m')

x_DP = - OCA_O4 / math.tan(8.3 * math.pi / 180) # m
y_DP = 0;

print("OCA_O1: "+ str(OCA_O1) + " m")
print("OCA_O2: "+ str(OCA_O2) + " m")

PinS_to_HRP = 1 * 1852 # m

x_alpha = -(60 - Safety_AW)/math.tan(15 * math.pi / 180) - Safety_AW

plt.figure(0)

plt.scatter(x_DP,y_DP,s=20, c='k')
plt.annotate('DP', (x_DP + 20, y_DP + 20))

plt.scatter(-PinS_to_HRP,0)
plt.annotate('MAPt', (-PinS_to_HRP,0))

plt.scatter(-O1[0]-PinS_to_HRP,O1[1])
plt.annotate('O1', (-O1[0]-PinS_to_HRP,O1[1]))

plt.scatter(-O2[0]-PinS_to_HRP,O2[1])
plt.annotate('O2', (-O2[0]-PinS_to_HRP,O2[1]))

plt.plot(np.linspace(0, -PinS_to_HRP-4000, 1000),np.linspace(0, 0, 1000),'k', linewidth=0.7)

plt.plot(np.linspace(0, -PinS_to_HRP-4000, 1000), np.linspace(-0.4*1852, -0.4*1852, 1000), 'k--', linewidth=0.7)
plt.plot(np.linspace(0, -PinS_to_HRP-4000, 1000), np.linspace(0.4*1852, 0.4*1852, 1000), 'k--', linewidth=0.7)

plt.plot(np.linspace(-PinS_to_HRP, -PinS_to_HRP-4000, 1000), np.linspace(-0.8*1852, -0.8*1852, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(-PinS_to_HRP, -PinS_to_HRP-4000, 1000), np.linspace(0.8*1852, 0.8*1852, 1000), 'k', linewidth=0.7)

x_r = np.ones((1000, 1))
y_r = np.ones((1000, 1))
theta = np.linspace(-90, 90, 1000)

for i in range(0,1000):
    x_r[i] = (0.4 * 1852) * math.cos(theta[i] * math.pi / 180)
    y_r[i] = (0.4 * 1852) * math.sin(theta[i] * math.pi / 180)

plt.plot(x_r,y_r, 'k', linewidth=0.7)

sin_alpha = 0.4 * 1852 / (PinS_to_HRP ** 2 + (1852 * 0.8) ** 2) ** 0.5
sin_beta = PinS_to_HRP / (PinS_to_HRP ** 2 + (1852 * 0.8) ** 2) ** 0.5

alpha = np.arcsin(sin_alpha) * 180 / math.pi
beta = np.arcsin(sin_beta) * 180 / math.pi
gamma = 90 - (alpha + beta)

plt.plot(np.linspace(1000, -PinS_to_HRP, 1000),
         np.linspace(0.8*1852 - math.tan(gamma * math.pi / 180) * abs(PinS_to_HRP + 1000)
         , 0.8*1852, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(1000, -PinS_to_HRP, 1000),
         np.linspace(-0.8*1852 + math.tan(gamma * math.pi / 180) * abs(PinS_to_HRP + 1000)
         , -0.8*1852, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(x_DP, -PinS_to_HRP - ATT, 1000), np.linspace(-0.4*1852, -0.4*1852, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(x_DP, -PinS_to_HRP - ATT, 1000), np.linspace(0.4*1852, 0.4*1852, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(x_DP, x_DP, 1000), np.linspace(0.4*1852, -0.4*1852, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(-Safety_AW, x_DP, 1000), np.linspace(Safety_AW, 0.4*1852, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(-Safety_AW, x_DP, 1000), np.linspace(-Safety_AW, -0.4*1852, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(-PinS_to_HRP - ATT, -PinS_to_HRP - ATT, 1000), np.linspace(-0.4*1852, 0.4*1852, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(0, -PinS_to_HRP - ATT, 1000), np.linspace(0, 0, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(-PinS_to_HRP, -PinS_to_HRP, 1000), np.linspace(0.4*1852, 0.8*1852, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(-PinS_to_HRP, -PinS_to_HRP, 1000), np.linspace(-0.4*1852, -0.8*1852, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(x_DP, x_alpha, 1000), np.linspace(60, 60, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(x_DP, x_alpha, 1000), np.linspace(-60, -60, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(-Safety_AW, x_alpha, 1000), np.linspace(Safety_AW, 60, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(-Safety_AW, x_alpha, 1000), np.linspace(-Safety_AW, -60, 1000), 'k', linewidth=0.7)

plt.scatter(-O3[0],O3[1])
plt.annotate('O3', (-O3[0],O3[1]))

plt.scatter(-O4[0],O4[1])
plt.annotate('O4', (-O4[0],O4[1]))

plt.scatter(-O5[0],O5[1])
plt.annotate('O5', (-O5[0],O5[1]))

plt.scatter(-O6[0],O6[1])
plt.annotate('O6', (-O6[0],O6[1]))

plt.scatter(0,0)
plt.annotate('HRP', (0,0))

plt.scatter(-PinS_to_HRP,0)
plt.annotate('MAPt', (-PinS_to_HRP,0))

plt.grid(color='k', linestyle='-', linewidth=0.1)
plt.axis('equal')

# =========== Figure 1 ==============

plt.figure(1)

plt.plot(np.linspace(1000, -PinS_to_HRP, 1000),
         np.linspace(0.8*1852 - math.tan(gamma * math.pi / 180) * abs(PinS_to_HRP + 1000)
         , 0.8*1852, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(1000, -PinS_to_HRP, 1000),
         np.linspace(-0.8*1852 + math.tan(gamma * math.pi / 180) * abs(PinS_to_HRP + 1000)
         , -0.8*1852, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(-Safety_AW, x_DP, 1000), np.linspace(Safety_AW, 0.4*1852, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(0, -PinS_to_HRP, 1000), np.linspace(-0.4*1852, -0.4*1852, 1000), 'k--', linewidth=0.7)
plt.plot(np.linspace(0, -PinS_to_HRP, 1000), np.linspace(0.4*1852, 0.4*1852, 1000), 'k--', linewidth=0.7)
plt.plot(np.linspace(-Safety_AW, x_DP, 1000), np.linspace(-Safety_AW, -0.4*1852, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(x_DP, -PinS_to_HRP - ATT, 1000), np.linspace(-0.4*1852, -0.4*1852, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(x_DP, -PinS_to_HRP - ATT, 1000), np.linspace(0.4*1852, 0.4*1852, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(-PinS_to_HRP - ATT, -PinS_to_HRP - ATT, 1000), np.linspace(-0.4*1852, 0.4*1852, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(0, -PinS_to_HRP - ATT, 1000), np.linspace(0, 0, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(x_DP, -PinS_to_HRP, 1000), np.linspace(0.4*1852, 0.4*1852, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(x_DP, -PinS_to_HRP, 1000), np.linspace(-0.4*1852, -0.4*1852, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(x_alpha, x_DP, 1000), np.linspace(-60, -60, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(x_alpha, x_DP, 1000), np.linspace(60, 60, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(x_DP, x_DP, 1000), np.linspace(-60, -0.4*1852, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(x_DP, x_DP, 1000), np.linspace(60, 0.4*1852, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(x_DP, x_DP, 1000), np.linspace(0.4*1852, -0.4*1852, 1000), 'k--', linewidth=0.7)

plt.plot(np.linspace(-PinS_to_HRP, -PinS_to_HRP, 1000), np.linspace(0.4*1852, 0.8*1852, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(-PinS_to_HRP, -PinS_to_HRP, 1000), np.linspace(-0.4*1852, -0.8*1852, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(-Safety_AW, x_alpha, 1000), np.linspace(Safety_AW, 60, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(-Safety_AW, x_alpha, 1000), np.linspace(-Safety_AW, -60, 1000), 'k', linewidth=0.7)
plt.fill_between(np.linspace(-Safety_AW, x_alpha, 1000), np.linspace(Safety_AW, 60, 1000), np.linspace(-Safety_AW, -60, 1000),color='grey')

plt.plot(np.linspace(x_DP, x_alpha, 1000), np.linspace(60, 60, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(x_DP, x_alpha, 1000), np.linspace(-60, -60, 1000), 'k', linewidth=0.7)
plt.fill_between(np.linspace(x_DP, x_alpha, 1000), np.linspace(-60, -60, 1000), np.linspace(60, 60, 1000),color='grey')

plt.scatter(-O3[0],O3[1],s = 20)
plt.annotate('O3', (-O3[0] + 20,O3[1] + 20))

plt.scatter(-O4[0],O4[1],s = 20)
plt.annotate('O4', (-O4[0] + 20,O4[1] + 20))

plt.scatter(-O5[0],O5[1],s = 20)
plt.annotate('O5', (-O5[0] + 20,O5[1] + 20))

plt.scatter(-O6[0],O6[1],s = 20)
plt.annotate('O6', (-O6[0] + 20,O6[1] + 20))

plt.scatter(x_DP,y_DP,s = 20, c = 'k')
plt.annotate('DP', (x_DP + 20, y_DP + 20))

plt.scatter(0,0,s=20, c='k')
plt.annotate('HRP', (0+20,0+20))

plt.scatter(-PinS_to_HRP,0,s=20, c='k')
plt.annotate('MAPt', (-PinS_to_HRP+20,0+20))

plt.plot(x_r,y_r, 'k', linewidth=0.7)

plt.grid(color='k', linestyle='-', linewidth=0.1)
plt.axis('equal')

Surface_Height_O3 = (O3[0] - Safety_AW) * 0.125
Surface_Height_O4 = OCS_level
Surface_Height_O5 = (O5[0] - Safety_AW) * 0.125

print("Surface_Height_O3: "+ str(Surface_Height_O3)+ ' m')
print("Surface_Height_O4: "+ str(Surface_Height_O4)+ ' m')
print("Surface_Height_O5: "+ str(Surface_Height_O5)+ ' m')

plt.plot(np.linspace(-Safety_AW, -Safety_AW, 50), np.linspace(Safety_AW, -Safety_AW, 50), 'k', linewidth=0.7)

# =========== Figure(2) ==============

plt.figure(2)

buffer = 0.4 * 1852 # m

if OCH < 183:
    r_base_turn = 1482 # m
else:
    r_base_turn = 1852 + 185 * math.ceil((OCH - 183) / 30)

if OCH < 183:
    alpha_base_turn = 50  # deg
elif OCH < 304:
    alpha_base_turn = 50 + 5 * math.ceil((OCH - 183) / 30)
else:
    alpha_base_turn = 30

x_m = np.ones((1000, 1))
y_m = np.ones((1000, 1))
x_m_2 = np.ones((1000, 1))
y_m_2 = np.ones((1000, 1))
theta_m = np.linspace(-alpha_base_turn, alpha_base_turn, 1000)

for i in range(0, 1000):
    x_m[i] = (r_base_turn + buffer) * math.cos(theta_m[i] * math.pi / 180)
    y_m[i] = (r_base_turn + buffer) * math.sin(theta_m[i] * math.pi / 180)
    x_m_2[i] = r_base_turn * math.cos(theta_m[i] * math.pi / 180)
    y_m_2[i] = r_base_turn * math.sin(theta_m[i] * math.pi / 180)

plt.plot(x_m, y_m, 'k', linewidth=0.7)
plt.plot(x_m_2, y_m_2, 'k--', linewidth=0.7)


plt.plot(np.linspace(r_base_turn, -PinS_to_HRP, 1000), np.linspace(0, 0, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(x_m_2[999] , -PinS_to_HRP, 1000), np.linspace(y_m_2[999], 0, 1000), 'k--', linewidth=0.7)
plt.plot(np.linspace(x_m_2[0] , -PinS_to_HRP, 1000), np.linspace(y_m_2[0], 0, 1000), 'k--', linewidth=0.7)

gamma = math.atan(y_m_2[0] / (x_m_2[0] + PinS_to_HRP))

plt.plot(np.linspace(math.sin(gamma) * buffer - PinS_to_HRP, -PinS_to_HRP, 1000),
         np.linspace(math.cos(gamma) * buffer, buffer / math.cos(gamma), 1000),
         'k', linewidth=0.7)

plt.plot(np.linspace(math.sin(gamma) * buffer - PinS_to_HRP, -PinS_to_HRP, 1000),
         np.linspace(-math.cos(gamma) * buffer, -buffer / math.cos(gamma), 1000),
         'k', linewidth=0.7)

x_corner_top = np.ones((1000, 1))
y_corner_top = np.ones((1000, 1))
x_corner_bottom = np.ones((1000, 1))
y_corner_bottom  = np.ones((1000, 1))
theta_corner = np.linspace(90 + 3 * (gamma / math.pi * 180), 90 - (gamma / math.pi * 180), 1000)

for i in range(0, 1000):
    x_corner_bottom[i] = x_m_2[0] + buffer * math.cos(-theta_corner[i] * math.pi / 180)
    y_corner_bottom[i] = y_m_2[0] + buffer * math.sin(-theta_corner[i] * math.pi / 180)
    x_corner_top[i] = x_m_2[999] + buffer * math.cos(theta_corner[i] * math.pi / 180)
    y_corner_top[i] = y_m_2[999] + buffer * math.sin(theta_corner[i] * math.pi / 180)

plt.plot(x_corner_bottom, y_corner_bottom, 'k', linewidth=0.7)
plt.plot(x_corner_top, y_corner_top, 'k', linewidth=0.7)

x3 = x_corner_top[999]
y3 = y_corner_top[999]

plt.scatter(x3,y3)

plt.plot(np.linspace(x_corner_top[999] , -PinS_to_HRP, 1000),
         np.linspace(y_corner_top[999] , buffer / math.cos(gamma), 1000),
         'k', linewidth=0.7)
plt.plot(np.linspace(x_corner_bottom[999] , -PinS_to_HRP, 1000),
         np.linspace(y_corner_bottom[999] , -buffer/ math.cos(gamma), 1000),
         'k', linewidth=0.7)



x_MAPt_buffer = np.ones((1000, 1))
y_MAPt_buffer = np.ones((1000, 1))
theta_MAPt_buffer = np.linspace(90 - gamma * 180 / math.pi, 270 + gamma * 180 / math.pi, 1000)

for i in range(0, 1000):
    x_MAPt_buffer[i] = -PinS_to_HRP + buffer * math.cos(theta_MAPt_buffer[i] * math.pi / 180)
    y_MAPt_buffer[i] = buffer * math.sin(theta_MAPt_buffer[i] * math.pi / 180)

plt.plot(x_MAPt_buffer, y_MAPt_buffer, 'k', linewidth=0.7)

plt.scatter(-O3[0],O3[1],s = 20)
plt.annotate('O3', (-O3[0] + 20,O3[1] + 20))

plt.scatter(-O4[0],O4[1],s = 20)
plt.annotate('O4', (-O4[0] + 20,O4[1] + 20))

plt.scatter(-O5[0],O5[1],s = 20)
plt.annotate('O5', (-O5[0] + 20,O5[1] + 20))

plt.scatter(-O6[0],O6[1],s = 20)
plt.annotate('O6', (-O6[0] + 20,O6[1] + 20))

plt.scatter(x_DP,y_DP,s = 20, c = 'k')
plt.annotate('DP', (x_DP + 20, y_DP + 20))

plt.scatter(0,0,s=20, c='k')
plt.annotate('HRP', (0+20,0+20))

plt.scatter(-PinS_to_HRP,0,s=20, c='k')
plt.annotate('MAPt', (-PinS_to_HRP+20,0+20))

plt.grid(color='k', linestyle='-', linewidth=0.1)
plt.axis('equal')


plt.show()