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

OCS_gradient = 0.125

VSDG_gradient = 0.133

x_IDF = 0.8 * 1852 # m

MOC = 30 # m

x_alpha = -(60 - Safety_AW)/math.tan(15 * math.pi / 180) - Safety_AW

MCA_IDF = VSDG_gradient * (x_IDF - Safety_AW)

OCS_surf_height_at_O3 = (O3[0] - Safety_AW) * OCS_gradient + HCH

print("OCS_surf_height_at_O3: " + str(OCS_surf_height_at_O3))

OIS_gradient = (MCA_IDF - MOC) / (x_IDF - Safety_AW)

print("MCA_IDF: "+ str(MCA_IDF))

print("OIS_gradient: "+ str(OIS_gradient))

y_IDF = 0;

plt.plot(np.linspace(-x_IDF - ATT, -x_IDF - ATT, 1000), np.linspace(-0.4*1852, 0.4*1852, 1000), 'k--', linewidth=0.7)
plt.plot(np.linspace(-x_IDF + ATT, -x_IDF + ATT, 1000), np.linspace(-0.4 * 1852, 0.4 * 1852, 1000), 'k--', linewidth=0.7)
plt.plot(np.linspace(-x_IDF, -x_IDF, 1000), np.linspace(-0.4 * 1852, 0.4 * 1852, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(0, -x_IDF - 6000, 1000), np.linspace(0, 0, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(-Safety_AW, x_alpha, 1000), np.linspace(Safety_AW, 60, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(-Safety_AW, x_alpha, 1000), np.linspace(-Safety_AW, -60, 1000), 'k', linewidth=0.7)
plt.fill_between(np.linspace(-Safety_AW, x_alpha, 1000), np.linspace(Safety_AW, 60, 1000), np.linspace(-Safety_AW, -60, 1000),color='grey')

plt.plot(np.linspace(-x_IDF, x_alpha, 1000), np.linspace(60, 60, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(-x_IDF, x_alpha, 1000), np.linspace(-60, -60, 1000), 'k', linewidth=0.7)
plt.fill_between(np.linspace(-x_IDF, x_alpha, 1000), np.linspace(-60, -60, 1000), np.linspace(60, 60, 1000),color='grey')

plt.plot(np.linspace(-(0.8 * 1852 - 45)/math.tan(15 * math.pi / 180),-Safety_AW, 1000), np.linspace(-0.8 * 1852, -45, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(-(0.8 * 1852 - 45)/math.tan(15 * math.pi / 180),-Safety_AW, 1000), np.linspace(0.8 * 1852, 45, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(-(0.4 * 1852 - 45)/math.tan(15 * math.pi / 180), -x_IDF - 6000, 1000), np.linspace(-0.4*1852, -0.4*1852, 1000), 'k--', linewidth=0.7)
plt.plot(np.linspace(-(0.4 * 1852 - 45)/math.tan(15 * math.pi / 180), -x_IDF - 6000, 1000), np.linspace(0.4*1852, 0.4*1852, 1000), 'k--', linewidth=0.7)

plt.plot(np.linspace(-(0.8 * 1852 - 45)/math.tan(15 * math.pi / 180), -x_IDF - 6000, 1000), np.linspace(-0.8*1852, -0.8*1852, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(-(0.8 * 1852 - 45)/math.tan(15 * math.pi / 180), -x_IDF - 6000, 1000), np.linspace(0.8*1852, 0.8*1852, 1000), 'k', linewidth=0.7)

plt.scatter(-O1[0]-x_IDF,O1[1])
plt.annotate('O1', (-O1[0]-x_IDF,O1[1]))

plt.scatter(-O2[0]-x_IDF,O2[1])
plt.annotate('O2', (-O2[0]-x_IDF,O2[1]))

plt.scatter(-O3[0],O3[1],s = 20)
plt.annotate('O3', (-O3[0] + 20,O3[1] + 20))

plt.scatter(-O4[0],O4[1],s = 20)
plt.annotate('O4', (-O4[0] + 20,O4[1] + 20))

plt.scatter(-O5[0],O5[1],s = 20)
plt.annotate('O5', (-O5[0] + 20,O5[1] + 20))

plt.scatter(-O6[0],O6[1],s = 20)
plt.annotate('O6', (-O6[0] + 20,O6[1] + 20))

plt.scatter(-x_IDF,y_IDF,s=20, c='k')
plt.annotate('IDF', (-x_IDF+20,y_IDF+20))

plt.grid(color='k', linestyle='-', linewidth=0.1)
plt.axis('equal')

plt.plot(np.linspace(-Safety_AW, -Safety_AW, 50), np.linspace(Safety_AW, -Safety_AW, 50), 'k', linewidth=0.7)

# =========== Figure(2) ==============

plt.figure(2)

MCH = MCA_IDF

buffer = 0.4 * 1852 # m

print("MCH: " + str(MCH))

if MCH < 183:
    r_base_turn = 1482 # m
else:
    r_base_turn = 1482 + 185 * math.ceil((MCH - 183) / 30)

print("r_base_turn: " + str(r_base_turn))

if MCH < 183:
    alpha_base_turn = 50  # deg
elif MCH < 304:
    alpha_base_turn = 50 + 5 * math.ceil((MCH - 183) / 30)
else:
    alpha_base_turn = 30

print("alpha_base_turn: " + str(alpha_base_turn))

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

plt.plot(np.linspace(r_base_turn, -x_IDF, 1000), np.linspace(0, 0, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(x_m_2[999] , -x_IDF, 1000), np.linspace(y_m_2[999], 0, 1000), 'k--', linewidth=0.7)
plt.plot(np.linspace(x_m_2[0] , -x_IDF, 1000), np.linspace(y_m_2[0], 0, 1000), 'k--', linewidth=0.7)

gamma = math.atan(y_m_2[0] / (x_m_2[0] + x_IDF))

plt.plot(np.linspace(math.sin(gamma) * buffer - x_IDF, -x_IDF, 1000),
         np.linspace(math.cos(gamma) * buffer, buffer / math.cos(gamma), 1000),
         'k', linewidth=0.7)

plt.plot(np.linspace(math.sin(gamma) * buffer - x_IDF, -x_IDF, 1000),
         np.linspace(-math.cos(gamma) * buffer, -buffer / math.cos(gamma), 1000),
         'k', linewidth=0.7)

x_corner_top = np.ones((1000, 1))
y_corner_top = np.ones((1000, 1))
x_corner_bottom = np.ones((1000, 1))
y_corner_bottom = np.ones((1000, 1))
theta_corner = np.linspace(90 + 1.5 * (gamma / math.pi * 180), 90 - (gamma / math.pi * 180), 1000)

for i in range(0, 1000):
    x_corner_bottom[i] = x_m_2[0] + buffer * math.cos(-theta_corner[i] * math.pi / 180)
    y_corner_bottom[i] = y_m_2[0] + buffer * math.sin(-theta_corner[i] * math.pi / 180)
    x_corner_top[i] = x_m_2[999] + buffer * math.cos(theta_corner[i] * math.pi / 180)
    y_corner_top[i] = y_m_2[999] + buffer * math.sin(theta_corner[i] * math.pi / 180)

plt.plot(x_corner_bottom , y_corner_bottom, 'k', linewidth=0.7)
plt.plot(x_corner_top, y_corner_top, 'k', linewidth=0.7)

x3 = x_corner_top[999]
y3 = y_corner_top[999]

plt.plot(np.linspace(x_corner_top[999] , -x_IDF, 1000),
         np.linspace(y_corner_top[999] , buffer / math.cos(gamma), 1000),
         'k', linewidth=0.7)
plt.plot(np.linspace(x_corner_bottom[999] , -x_IDF, 1000),
         np.linspace(y_corner_bottom[999] , -buffer/ math.cos(gamma), 1000),
         'k', linewidth=0.7)

x_MAPt_buffer = np.ones((1000, 1))
y_MAPt_buffer = np.ones((1000, 1))
theta_MAPt_buffer = np.linspace(90 - gamma * 180 / math.pi, 270 + gamma * 180 / math.pi, 1000)

for i in range(0, 1000):
    x_MAPt_buffer[i] = -x_IDF + buffer * math.cos(theta_MAPt_buffer[i] * math.pi / 180)
    y_MAPt_buffer[i] = buffer * math.sin(theta_MAPt_buffer[i] * math.pi / 180)

plt.plot(x_MAPt_buffer, y_MAPt_buffer, 'k', linewidth=0.7)

plt.plot(np.linspace(-Safety_AW, x_alpha, 1000), np.linspace(Safety_AW, 60, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(-Safety_AW, x_alpha, 1000), np.linspace(-Safety_AW, -60, 1000), 'k', linewidth=0.7)
plt.fill_between(np.linspace(-Safety_AW, x_alpha, 1000), np.linspace(Safety_AW, 60, 1000), np.linspace(-Safety_AW, -60, 1000),color='grey')

plt.plot(np.linspace(-x_IDF, x_alpha, 1000), np.linspace(60, 60, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(-x_IDF, x_alpha, 1000), np.linspace(-60, -60, 1000), 'k', linewidth=0.7)
plt.fill_between(np.linspace(-x_IDF, x_alpha, 1000), np.linspace(-60, -60, 1000), np.linspace(60, 60, 1000),color='grey')

plt.scatter(-O3[0],O3[1],s = 20)
plt.annotate('O3', (-O3[0] + 20,O3[1] + 20))

plt.scatter(-O4[0],O4[1],s = 20)
plt.annotate('O4', (-O4[0] + 20,O4[1] + 20))

plt.scatter(-O5[0],O5[1],s = 20)
plt.annotate('O5', (-O5[0] + 20,O5[1] + 20))

plt.scatter(-O6[0],O6[1],s = 20)
plt.annotate('O6', (-O6[0] + 20,O6[1] + 20))

plt.scatter(-x_IDF,y_IDF,s=20, c='k')
plt.annotate('IDF', (-x_IDF+20,y_IDF+20))

plt.scatter(0,0,s=20, c='k')
plt.annotate('HRP', (0+20,0+20))

plt.grid(color='k', linestyle='-', linewidth=0.1)
plt.axis('equal')

# ============ Figure 3 =================== #

plt.figure(3)

plt.plot(np.linspace(-x_IDF - ATT, -x_IDF - ATT, 1000), np.linspace(-0.4*1852, 0.4*1852, 1000), 'k--', linewidth=0.7)
plt.plot(np.linspace(-x_IDF + ATT, -x_IDF + ATT, 1000), np.linspace(-0.4 * 1852, 0.4 * 1852, 1000), 'k--', linewidth=0.7)
plt.plot(np.linspace(-x_IDF, -x_IDF, 1000), np.linspace(-0.4 * 1852, 0.4 * 1852, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(0, -x_IDF - 6000, 1000), np.linspace(0, 0, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(-Safety_AW, x_alpha, 1000), np.linspace(Safety_AW, 60, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(-Safety_AW, x_alpha, 1000), np.linspace(-Safety_AW, -60, 1000), 'k', linewidth=0.7)
plt.fill_between(np.linspace(-Safety_AW, x_alpha, 1000), np.linspace(Safety_AW, 60, 1000), np.linspace(-Safety_AW, -60, 1000),color='grey')

plt.plot(np.linspace(-x_IDF, x_alpha, 1000), np.linspace(60, 60, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(-x_IDF, x_alpha, 1000), np.linspace(-60, -60, 1000), 'k', linewidth=0.7)
plt.fill_between(np.linspace(-x_IDF, x_alpha, 1000), np.linspace(-60, -60, 1000), np.linspace(60, 60, 1000),color='grey')

plt.plot(np.linspace(-(0.8 * 1852 - 45)/math.tan(15 * math.pi / 180),-Safety_AW, 1000), np.linspace(-0.8 * 1852, -45, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(-(0.8 * 1852 - 45)/math.tan(15 * math.pi / 180),-Safety_AW, 1000), np.linspace(0.8 * 1852, 45, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(-(0.4 * 1852 - 45)/math.tan(15 * math.pi / 180), -x_IDF - 6000, 1000), np.linspace(-0.4*1852, -0.4*1852, 1000), 'k--', linewidth=0.7)
plt.plot(np.linspace(-(0.4 * 1852 - 45)/math.tan(15 * math.pi / 180), -x_IDF - 6000, 1000), np.linspace(0.4*1852, 0.4*1852, 1000), 'k--', linewidth=0.7)

plt.plot(np.linspace(-(0.8 * 1852 - 45)/math.tan(15 * math.pi / 180), -x_IDF - 6000, 1000), np.linspace(-0.8*1852, -0.8*1852, 1000), 'k', linewidth=0.7)
plt.plot(np.linspace(-(0.8 * 1852 - 45)/math.tan(15 * math.pi / 180), -x_IDF - 6000, 1000), np.linspace(0.8*1852, 0.8*1852, 1000), 'k', linewidth=0.7)

plt.scatter(-O1[0]-x_IDF,O1[1])
plt.annotate('O1', (-O1[0]-x_IDF,O1[1]))

plt.scatter(-O2[0]-x_IDF,O2[1])
plt.annotate('O2', (-O2[0]-x_IDF,O2[1]))

plt.scatter(-O3[0],O3[1],s = 20)
plt.annotate('O3', (-O3[0] + 20,O3[1] + 20))

plt.scatter(-O4[0],O4[1],s = 20)
plt.annotate('O4', (-O4[0] + 20,O4[1] + 20))

plt.scatter(-O5[0],O5[1],s = 20)
plt.annotate('O5', (-O5[0] + 20,O5[1] + 20))

plt.scatter(-O6[0],O6[1],s = 20)
plt.annotate('O6', (-O6[0] + 20,O6[1] + 20))

plt.scatter(-x_IDF,y_IDF,s=20, c='k')
plt.annotate('IDF', (-x_IDF+20,y_IDF+20))

plt.grid(color='k', linestyle='-', linewidth=0.1)
plt.axis('equal')

plt.plot(np.linspace(-Safety_AW, -Safety_AW, 50), np.linspace(Safety_AW, -Safety_AW, 50), 'k', linewidth=0.7)

MCH = MCA_IDF

buffer = 0.4 * 1852 # m

print("MCH: " + str(MCH))

if MCH < 183:
    r_base_turn = 1482 # m
else:
    r_base_turn = 1482 + 185 * math.ceil((MCH - 183) / 30)

print("r_base_turn: " + str(r_base_turn))

if MCH < 183:
    alpha_base_turn = 50  # deg
elif MCH < 304:
    alpha_base_turn = 50 + 5 * math.ceil((MCH - 183) / 30)
else:
    alpha_base_turn = 30

print("alpha_base_turn: " + str(alpha_base_turn))

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

plt.plot(np.linspace(r_base_turn, -x_IDF, 1000), np.linspace(0, 0, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(x_m_2[999] , -x_IDF, 1000), np.linspace(y_m_2[999], 0, 1000), 'k--', linewidth=0.7)
plt.plot(np.linspace(x_m_2[0] , -x_IDF, 1000), np.linspace(y_m_2[0], 0, 1000), 'k--', linewidth=0.7)

gamma = math.atan(y_m_2[0] / (x_m_2[0] + x_IDF))

plt.plot(np.linspace(math.sin(gamma) * buffer - x_IDF, -x_IDF, 1000),
         np.linspace(math.cos(gamma) * buffer, buffer / math.cos(gamma), 1000),
         'k', linewidth=0.7)

plt.plot(np.linspace(math.sin(gamma) * buffer - x_IDF, -x_IDF, 1000),
         np.linspace(-math.cos(gamma) * buffer, -buffer / math.cos(gamma), 1000),
         'k', linewidth=0.7)

x_corner_top = np.ones((1000, 1))
y_corner_top = np.ones((1000, 1))
x_corner_bottom = np.ones((1000, 1))
y_corner_bottom = np.ones((1000, 1))
theta_corner = np.linspace(90 + 1.5 * (gamma / math.pi * 180), 90 - (gamma / math.pi * 180), 1000)

for i in range(0, 1000):
    x_corner_bottom[i] = x_m_2[0] + buffer * math.cos(-theta_corner[i] * math.pi / 180)
    y_corner_bottom[i] = y_m_2[0] + buffer * math.sin(-theta_corner[i] * math.pi / 180)
    x_corner_top[i] = x_m_2[999] + buffer * math.cos(theta_corner[i] * math.pi / 180)
    y_corner_top[i] = y_m_2[999] + buffer * math.sin(theta_corner[i] * math.pi / 180)

plt.plot(x_corner_bottom , y_corner_bottom, 'k', linewidth=0.7)
plt.plot(x_corner_top, y_corner_top, 'k', linewidth=0.7)

x3 = x_corner_top[999]
y3 = y_corner_top[999]

plt.plot(np.linspace(x_corner_top[999] , -x_IDF, 1000),
         np.linspace(y_corner_top[999] , buffer / math.cos(gamma), 1000),
         'k', linewidth=0.7)
plt.plot(np.linspace(x_corner_bottom[999] , -x_IDF, 1000),
         np.linspace(y_corner_bottom[999] , -buffer/ math.cos(gamma), 1000),
         'k', linewidth=0.7)

x_MAPt_buffer = np.ones((1000, 1))
y_MAPt_buffer = np.ones((1000, 1))
theta_MAPt_buffer = np.linspace(90 - gamma * 180 / math.pi, 270 + gamma * 180 / math.pi, 1000)

for i in range(0, 1000):
    x_MAPt_buffer[i] = -x_IDF + buffer * math.cos(theta_MAPt_buffer[i] * math.pi / 180)
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

plt.scatter(-x_IDF,y_IDF,s=20, c='k')
plt.annotate('IDF', (-x_IDF+20,y_IDF+20))

plt.scatter(0,0,s=20, c='k')
plt.annotate('HRP', (0+20,0+20))

plt.grid(color='k', linestyle='-', linewidth=0.1)
plt.axis('equal')


# ============ Figure 4 Vertical =================== #

plt.figure(4)

MCA_IDF = 90
x_IDF = MCA_IDF / 0.133 # m

plt.plot(np.linspace(0, -x_IDF - ATT, 1000), np.linspace(0, 0, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(-Safety_AW, -x_IDF, 1000), np.linspace(0, MCA_IDF, 1000), 'k--', linewidth=0.7)

plt.plot(np.linspace(-Safety_AW, -x_IDF, 1000), np.linspace(0, MCA_IDF - MOC, 1000), 'k--', linewidth=0.7)

plt.plot(np.linspace(-Safety_AW, -(MCA_IDF - MOC) / 0.125, 1000), np.linspace(0, MCA_IDF - MOC, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(-x_IDF - ATT, -(MCA_IDF - MOC) / 0.125, 1000), np.linspace(MCA_IDF - MOC, MCA_IDF - MOC, 1000), 'k', linewidth=0.7)

plt.plot(np.linspace(-x_IDF, -x_IDF, 1000), np.linspace(MCA_IDF - 100, MCA_IDF + 50, 1000), 'k--', linewidth=0.7)

plt.plot(np.linspace(-x_IDF, -x_IDF + 300, 1000), np.linspace(MCA_IDF, MCA_IDF, 1000), 'k', linewidth=0.7)

plt.annotate('IDF', (-x_IDF,MCA_IDF + 50))

plt.annotate('IDF_MCA:' + str(MCA_IDF) + ' m', (-x_IDF + 20,MCA_IDF + 5))

plt.plot(np.linspace(-x_IDF - ATT, -x_IDF - ATT, 1000), np.linspace(MCA_IDF - 50, MCA_IDF + 50, 1000), 'k--', linewidth=0.7)

plt.grid(color='k', linestyle='-', linewidth=0.1)


plt.show()