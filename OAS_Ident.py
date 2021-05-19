import numpy as np
import math
import matplotlib.pyplot as plt
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

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

O1 = np.array([2700, 400, 17]) # O1 (x, y, z)
O2 = np.array([4000, 1350, 81]) # O2 (x, y, z)
O3 = np.array([500, 40, 19]) # O3
O4 = np.array([1000, 80, 30]) # O4
O5 = np.array([200, 150, 65]) # O5
O6 = np.array([-700, 300, 70]) # O6

point_O1 = Point(O1[0], O1[1]) # create point
point_O2 = Point(O2[0], O2[1]) # create point
point_O3 = Point(O3[0], O3[1]) # create point
point_O4 = Point(O4[0], O4[1]) # create point
point_O5 = Point(O5[0], O5[1]) # create point
point_O6 = Point(O6[0], O6[1]) # create point

O = np.array([O1, O2, O3, O4, O5, O6])

print(O)

x_O = O[:, 0]
y_O = O[:, 1]

xy_O = np.array([x_O, y_O])
print(xy_O)

W_Surface = [0.03494, 0, -6.45]
W2_Surface = [0.0485, 0, -38.77]
X_Surface = [0.032556, 0.2138, -58.89]
Y_Surface = [0.025341, 0.26772, -67.93]
Z_Surface = [-MA_Climb_Gradient, 0, -58.29]

OAS_Surface = np.array([W_Surface,
                       W2_Surface,
                       X_Surface,
                       Y_Surface,
                       Z_Surface])

print(OAS_Surface)

OAS_A = OAS_Surface[:, 0]
OAS_B = OAS_Surface[:, 1]
OAS_C = OAS_Surface[:, 2]

OAS_AB = np.array([OAS_A, OAS_B]).transpose()

print(OAS_AB)

print(OAS_C.transpose())

C = np.array([OAS_C,OAS_C,OAS_C,OAS_C,OAS_C,OAS_C]).transpose()

Surface_Height = OAS_AB.dot(xy_O) + C

print(Surface_Height)

# Foot Print Contour
C = [799.18 + 1052, 153.76]
D = [375.1 + 1052, 218.18, 218.14]
E = [-1387.63 + 1052, 485.75]

C1 = [8770.75 + 1052, 344, 300]
C2 = [2382.75 + 1052, 271.88, 74.81]
D1 = [5281.05 + 1052, 874.47, 300]
E1 = [-5747.08 + 1052, 1481.6, 183.1]

# Define X1 Surface
x_X1_Surface = np.array([C1[0],D1[0],D[0],C[0],C2[0],C1[0]])
y_X1_Surface = np.array([C1[1],D1[1],D[1],C[1],C2[1],C1[1]])
xy_X1_Surface = np.column_stack((x_X1_Surface, y_X1_Surface)) # Reshape coordinates
X1_Surface = Polygon(xy_X1_Surface) # create polygon

# Define X2 Surface
x_X2_Surface = np.array([C1[0],D1[0],D[0],C[0],C2[0],C1[0]])
y_X2_Surface = np.array([-C1[1],-D1[1],-D[1],-C[1],-C2[0],-C1[1]])
xy_X2_Surface = np.column_stack((x_X2_Surface, y_X2_Surface)) # Reshape coordinates
X2_Surface = Polygon(xy_X2_Surface) # create polygon

# Define Y1 Surface
x_Y1_Surface = np.array([D1[0],E1[0],E[0],D[0],D1[0]])
y_Y1_Surface = np.array([D1[1],E1[1],E[1],D[1],D1[1]])
xy_Y1_Surface = np.column_stack((x_Y1_Surface, y_Y1_Surface)) # Reshape coordinates
Y1_Surface = Polygon(xy_Y1_Surface) # create polygon

# Define Y2 Surface
x_Y2_Surface = np.array([D1[0],E1[0],E[0],D[0],D1[0]])
y_Y2_Surface = np.array([-D1[1],-E1[1],-E[1],-D[1],-D1[1]])
xy_Y2_Surface = np.column_stack((x_Y2_Surface, y_Y2_Surface)) # Reshape coordinates
Y2_Surface = Polygon(xy_Y2_Surface) # create polygon

# Define G Surface
x_G_Surface = np.array([D[0],E[0],E[0],D[0],D[0]])
y_G_Surface = np.array([D[1],E[1],-E[1],-D[1],D[1]])
xy_G_Surface = np.column_stack((x_G_Surface, y_G_Surface)) # Reshape coordinates
G_Surface = Polygon(xy_G_Surface) # create polygon

# Define W Surface
x_W_Surface = np.array([C[0],C1[0],C1[0],C[0],C[0]])
y_W_Surface = np.array([C[1],C1[1],-C1[1],-C1[1],C[1]])
xy_W_Surface = np.column_stack((x_W_Surface, y_W_Surface)) # Reshape coordinates
W_Surface = Polygon(xy_W_Surface) # create polygon

# Define Z Surface
x_Z_Surface = np.array([E[0],E1[0],E1[0],E[0],E[0]])
y_Z_Surface = np.array([E[1],E1[1],-E1[1],-E[1],E[1]])
xy_Z_Surface = np.column_stack((x_Z_Surface, y_Z_Surface)) # Reshape coordinates
Z_Surface = Polygon(xy_Z_Surface) # create polygon

print(Z_Surface.contains(point_O6)) # check if polygon contains point
print(point_O6.within(Z_Surface)) # check if a point is in the polygon