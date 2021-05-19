import numpy as np
import math
import matplotlib.pyplot as plt
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon


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

#O = np.array([O1, O2, O3, O4, O5, O6])

O = np.array([[2700, 400, 17],
              [4000, 1350, 81],
              [500, 40, 19],
              [1000, 80, 30],
              [200, 150, 65],
              [-700, 300, 70]])

for i in range(0, 6):
    plt.scatter(-O[i,0],O[i,1])
    plt.annotate('O'+str(i), (-O[i,0], O[i,1]))

plt.grid(color='k', linestyle='-', linewidth=0.1)
plt.axis('equal')

plt.savefig('foos.png')

