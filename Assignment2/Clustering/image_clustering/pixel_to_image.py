#!/usr/bin/env python
# This script takes a pixel csv located at <pixel_name> and transforms it back into an image. The image ist saved at
# <data_path>/<image_name>.

from PIL import Image

data_path = "data/"
pixel_name = "robocup_reduced.csv"
image_name = "robocup_recreated.jpg"

im = Image.new('RGB', (256, 385))

with open(data_path + pixel_name, 'r') as f:
    for i, line in enumerate(f.readlines()):
        if i != 0:
            line = line.rstrip('\n')
            split = line.split(',')
            value = (int(split[2]), int(split[3]), int(split[4]))
            im.putpixel((int(split[0]), int(split[1])), value)

im.save(data_path + image_name)
