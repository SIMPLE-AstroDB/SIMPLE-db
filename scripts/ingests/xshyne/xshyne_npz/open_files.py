#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 19 14:24:04 2025

@author: simonpetrus
"""

import numpy as np
import matplotlib.pyplot as plt

path = 'YOUR_PATH/2MASS1826.npz'


file_open = np.load(path)


wl = file_open['wl']
flx = file_open['flx']
err = file_open['err']

plt.errorbar(wl, flx, err)
plt.show()

