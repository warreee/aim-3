#!/usr/bin/env python
# This script takes an image located at <data_path>/<image_name> and transforms it into csv file, stored at
# <data_path>/<pixel_name>. Each row in the csv file contains the information about 1 pixel (location + rgb value).


from PIL import Image

data_path = "data/"
image_name = "robocup.jpg"
pixel_name = "robocup.csv"

im = Image.open(data_path + image_name)
pix = im.load()

# get a tuple of the x and y dimensions of the image
width, height = im.size

# open a file to write the pixel data
with open(data_path + pixel_name, 'w') as f:
    f.write('x,y,R,G,B\n')
    # read the details of each pixel and write them to the file
    for x in range(width):
        for y in range(height):
            r = pix[x, y][0]
            g = pix[x, y][1]
            b = pix[x, y][2]
            f.write('{0},{1},{2},{3},{4}\n'.format(x, y, r, g, b))
