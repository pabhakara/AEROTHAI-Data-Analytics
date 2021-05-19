import numpy as np
import math
import matplotlib.pyplot as plt
import subprocess
import shlex

# Define the RF Departure Leg

WPT_dist = 2.4 # NM
RF_radius = 5  # NM
Track_Change_Angle = 45  # degrees

# ------------------------

RF_arc_length = RF_radius * 2 * np.pi * Track_Change_Angle / 360

DER_to_end_of_RF = WPT_dist + RF_arc_length

print('RF Arc Length: ' + str(RF_arc_length) + ' NM')
print('DER to End of RF: ' + str(DER_to_end_of_RF) + ' NM')

Climb_gradient = 0.10  #
Altitude_at_end_of_RF = (DER_to_end_of_RF * 1852 * Climb_gradient + 5) * 3.2808

print('Altitude at End of RF: ' + str(Altitude_at_end_of_RF) + ' ft')

IAS = 238  # kt
Temp_VAR = 15  # C
Tailwind = 10  # kt

TAS = IAS * 171233 * ((288 + Temp_VAR) - 0.00198 * Altitude_at_end_of_RF) ** 0.5 / (
        288 - 0.00198 * Altitude_at_end_of_RF) ** 2.628

print('TAS: ' + str(TAS) + ' kt')

ATT = 0.8  # NM
Half_AW = 2.0  # NM
Reduced_ATT = (ATT * 1852 - 120) / ((Half_AW * 1852 - 150) / math.tan(15 / 180 * math.pi)) * WPT_dist * 1852 + 120

print('Reduced ATT ' + str(Reduced_ATT) + ' m')

AW_at_WPT = 150 + WPT_dist * 1852 * math.tan(15 / 180 * math.pi)

print('Area Width at WPT: '+ str(AW_at_WPT) +' m')

rs_inner = RF_radius * 1852 - AW_at_WPT
rs_outer = RF_radius * 1852 + AW_at_WPT

print('rs_inner: ' + str(rs_inner) +' m')
print('rs_outer: ' + str(rs_outer) +' m')

# Define parameters for the RF arc drawings

theta = np.linspace(0, Track_Change_Angle, Track_Change_Angle + 1)

x_inner = np.ones((Track_Change_Angle + 1, 1))
y_inner = np.ones((Track_Change_Angle + 1, 1))

x_outer = np.ones((Track_Change_Angle + 1, 1))
y_outer = np.ones((Track_Change_Angle + 1, 1))

x_inner_AW_edge = np.ones((Track_Change_Angle + 1, 1))
y_inner_AW_edge = np.ones((Track_Change_Angle + 1, 1))

x_outer_AW_edge = np.ones((Track_Change_Angle + 1, 1))
y_outer_AW_edge = np.ones((Track_Change_Angle + 1, 1))

x_nominal_in_RF = np.ones((Track_Change_Angle + 1, 1))
y_nominal_in_RF = np.ones((Track_Change_Angle + 1, 1))

x_nominal_straight = np.linspace(0, 0, 50)
y_nominal_straight = np.linspace(0, WPT_dist * 1852, 50)

x_left_spray = np.linspace(-150, -150 - math.tan(15 / 180 * math.pi) * WPT_dist * 1852, 50)
y_left_spray = np.linspace(0, WPT_dist * 1852, 50)

x_right_spray = np.linspace(150, 150 + math.tan(15 / 180 * math.pi) * WPT_dist * 1852, 50)
y_right_spray = np.linspace(0, WPT_dist * 1852, 50)

# ---- Define the dimension of the arcs in x-y coordinates -----

r_inner = rs_inner - math.tan(15 / 180 * math.pi) * theta / 180 * math.pi * rs_inner
r_outer = rs_outer + math.tan(15 / 180 * math.pi) * theta / 180 * math.pi * rs_outer
r_inner_AW_edge = (RF_radius - 1) * 1852
r_outer_AW_edge = (RF_radius + 1.05) * 1852

for i in range(0,Track_Change_Angle + 1):
    x_inner[i] = -math.cos(theta[i] * math.pi / 180) * r_inner[i] + RF_radius * 1852
    y_inner[i] = math.sin(theta[i] * math.pi / 180) * r_inner[i] + WPT_dist * 1852

    x_outer[i] = -math.cos(theta[i] * math.pi / 180) * r_outer[i] + RF_radius * 1852
    y_outer[i] = math.sin(theta[i] * math.pi / 180) * r_outer[i] + WPT_dist * 1852

    x_inner_AW_edge[i] = -math.cos(theta[i] * math.pi / 180) * r_inner_AW_edge + RF_radius * 1852
    y_inner_AW_edge[i] = math.sin(theta[i] * math.pi / 180) * r_inner_AW_edge + WPT_dist * 1852

    x_outer_AW_edge[i] = -math.cos(theta[i] * math.pi / 180) * r_outer_AW_edge + RF_radius * 1852
    y_outer_AW_edge[i] = math.sin(theta[i] * math.pi / 180) * r_outer_AW_edge + WPT_dist * 1852

    x_nominal_in_RF[i] = -math.cos(theta[i] * math.pi / 180) * RF_radius * 1852 + RF_radius * 1852
    y_nominal_in_RF[i] = math.sin(theta[i] * math.pi / 180) * RF_radius * 1852 + WPT_dist * 1852

x_nominal_in_RF_final = x_nominal_in_RF[Track_Change_Angle]
y_nominal_in_RF_final = y_nominal_in_RF[Track_Change_Angle]

x_inner_AW_edge_final = x_inner_AW_edge[Track_Change_Angle]
y_inner_AW_edge_final = y_inner_AW_edge[Track_Change_Angle]

x_outer_AW_edge_final = x_outer_AW_edge[Track_Change_Angle]
y_outer_AW_edge_final = y_outer_AW_edge[Track_Change_Angle]

x_inner_final = x_inner[Track_Change_Angle]
y_inner_final = y_inner[Track_Change_Angle]

x_outer_final = x_outer[Track_Change_Angle]
y_outer_final = y_outer[Track_Change_Angle]

x_nominal_straight_2 = np.linspace(x_nominal_in_RF_final, x_nominal_in_RF_final + math.sin(Track_Change_Angle * math.pi / 180) * 5 * 1852, 50)
y_nominal_straight_2 = np.linspace(y_nominal_in_RF_final, y_nominal_in_RF_final + math.cos(Track_Change_Angle * math.pi / 180) * 5 * 1852, 50)

x_inner_AW_edge_2 = np.linspace(x_inner_AW_edge_final, x_inner_AW_edge_final + math.sin(Track_Change_Angle * math.pi / 180) * 5 * 1852, 50)
y_inner_AW_edge_2 = np.linspace(y_inner_AW_edge_final, y_inner_AW_edge_final + math.cos(Track_Change_Angle * math.pi / 180) * 5 * 1852, 50)

x_outer_AW_edge_2 = np.linspace(x_outer_AW_edge_final, x_outer_AW_edge_final + math.sin(Track_Change_Angle * math.pi / 180) * 5 * 1852, 50)
y_outer_AW_edge_2 = np.linspace(y_outer_AW_edge_final, y_outer_AW_edge_final + math.cos(Track_Change_Angle * math.pi / 180) * 5 * 1852, 50)

x_inner_2 = np.linspace(x_inner_final, x_inner_final + math.sin(Track_Change_Angle * math.pi / 180) * 5 * 1852, 50)
y_inner_2 = np.linspace(y_inner_final, y_inner_final + math.cos(Track_Change_Angle * math.pi / 180) * 5 * 1852, 50)

x_outer_2 = np.linspace(x_outer_final, x_outer_final + math.sin(Track_Change_Angle * math.pi / 180) * 5 * 1852, 50)
y_outer_2 = np.linspace(y_outer_final, y_outer_final + math.cos(Track_Change_Angle * math.pi / 180) * 5 * 1852, 50)

plt.plot(x_inner, y_inner, color='blue')
plt.plot(x_outer, y_outer, color='blue')

plt.plot(x_inner_2, y_inner_2, 'b--')
plt.plot(x_outer_2, y_outer_2, 'b--')

plt.plot(x_inner_AW_edge[0:Track_Change_Angle+1], y_inner_AW_edge[0:Track_Change_Angle+1], color='red')
plt.plot(x_inner_AW_edge_2, y_inner_AW_edge_2, color='red')

plt.plot(x_outer_AW_edge[0:Track_Change_Angle+1], y_outer_AW_edge[0:Track_Change_Angle+1], color='red')
plt.plot(x_outer_AW_edge_2, y_outer_AW_edge_2, color='red')

plt.plot(x_left_spray, y_left_spray, color='blue')
plt.plot(x_right_spray, y_right_spray, color='blue')

plt.plot(x_nominal_straight, y_nominal_straight, color='black', linewidth=0.7)
plt.plot(x_nominal_in_RF, y_nominal_in_RF, color='black', linewidth=0.7)
plt.plot(x_nominal_straight_2, y_nominal_straight_2, color='black', linewidth=0.7)

x_O1 = 3700 # m
y_O1 = 650 # m

x_O2 = 12250 # m
y_O2 = 6250 # m

r_O4 = 1000 # m
theta_O4 = 27 # deg
x_O4 = x_nominal_in_RF[theta_O4] - r_O4 * math.cos(theta_O4 * math.pi / 180)
y_O4 = y_nominal_in_RF[theta_O4] + r_O4 * math.sin(theta_O4 * math.pi / 180)

r_O5 = 2500 # m
theta_O5 = 27 # deg
x_O5 = x_nominal_in_RF[theta_O5] - r_O5 * math.cos(theta_O5 * math.pi / 180)
y_O5 = y_nominal_in_RF[theta_O5] + r_O5 * math.sin(theta_O5 * math.pi / 180)

r_O6 = 1200 # m
theta_O6 = 34 # deg
x_O6 = x_nominal_in_RF[theta_O6] + r_O6 * math.cos(theta_O6 * math.pi / 180)
y_O6 = y_nominal_in_RF[theta_O6] - r_O6 * math.sin(theta_O6 * math.pi / 180)

plt.scatter(y_O1,x_O1)
plt.annotate('O1', (y_O1,x_O1))
plt.scatter(y_O2,x_O2)
plt.annotate('O2', (y_O2,x_O2))
plt.scatter(x_O4,y_O4)
plt.annotate('O4', (x_O4,y_O4))
plt.scatter(x_O5,y_O5)
plt.annotate('O5', (x_O5,y_O5))
plt.scatter(x_O6,y_O6)
plt.annotate('O6', (x_O6,y_O6))

WPT_dist = 2.4 # NM
RF_radius = 5  # NM

plt.scatter(RF_radius * 1852,WPT_dist * 1852)
plt.annotate('Arc Center', (RF_radius * 1852, WPT_dist * 1852))

x_arc_start = np.linspace(RF_radius * 1852, 0, 50)
y_arc_start = np.linspace(WPT_dist * 1852, WPT_dist * 1852, 50)

x_arc_end = np.linspace(RF_radius * 1852, x_nominal_in_RF_final, 50)
y_arc_end = np.linspace(WPT_dist * 1852, y_nominal_in_RF_final, 50)


print('x_nominal_in_RF_final: ' + str(x_nominal_in_RF_final) +' m')
print('y_nominal_in_RF_final: ' + str(y_nominal_in_RF_final) +' m')

plt.plot(x_arc_start, y_arc_start, 'k--', linewidth=0.7)
plt.plot(x_arc_end, y_arc_end, 'k--', linewidth=0.7)

plt.grid(color='k', linestyle='-', linewidth=0.1)
plt.axis('equal')

fname = '/tmp/test.pdf'
plt.savefig(fname)
proc = subprocess.Popen(shlex.split('lpr {f}'.format(f=fname)))

plt.show()