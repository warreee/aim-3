import graphlab as gl

data_path = "/home/warreee/projects/2016-SS-Assignments/Assignment2/Clustering/image_clustering/data/"

pixel_name = "robocup_reduced.csv"

pixels = gl.SFrame.read_csv("data/robocup.csv")

# Change the second parameter of this method here: 2, 4 or 8.
model = gl.kmeans.create(pixels, 8, ["R", "G", "B"], max_iterations=100)

processed_data = model.get("cluster_id")
clusters = model.get("cluster_info")
pixel_name = "robocup_reduced8.csv"
# Warning, takes a long time to write, due to inefficiency
with open(data_path + pixel_name, 'w') as f:
    f.write('x,y,R,G,B\n')
    i = 0
    for row in processed_data:
        index = row.get('row_id')
        cluster = row.get('cluster_id')
        x = pixels[index].get('x')
        y = pixels[index].get('y')
        r = int(clusters[cluster].get('R'))
        g = int(clusters[cluster].get('G'))
        b = int(clusters[cluster].get('B'))
        f.write('{0},{1},{2},{3},{4}\n'.format(x, y, r, g, b))

print model.summary()

# Change the second parameter of this method here: 2, 4 or 8.
model = gl.kmeans.create(pixels, 4, ["R", "G", "B"], max_iterations=100)

processed_data = model.get("cluster_id")
clusters = model.get("cluster_info")
pixel_name = "robocup_reduced4.csv"
# Warning, takes a long time to write, due to inefficiency
with open(data_path + pixel_name, 'w') as f:
    f.write('x,y,R,G,B\n')
    i = 0
    for row in processed_data:
        index = row.get('row_id')
        cluster = row.get('cluster_id')
        x = pixels[index].get('x')
        y = pixels[index].get('y')
        r = int(clusters[cluster].get('R'))
        g = int(clusters[cluster].get('G'))
        b = int(clusters[cluster].get('B'))
        f.write('{0},{1},{2},{3},{4}\n'.format(x, y, r, g, b))

print model.summary()

# Change the second parameter of this method here: 2, 4 or 8.
model = gl.kmeans.create(pixels, 2, ["R", "G", "B"], max_iterations=100)

processed_data = model.get("cluster_id")
clusters = model.get("cluster_info")
pixel_name = "robocup_reduced2.csv"
# Warning, takes a long time to write, due to inefficiency
with open(data_path + pixel_name, 'w') as f:
    f.write('x,y,R,G,B\n')
    i = 0
    for row in processed_data:
        index = row.get('row_id')
        cluster = row.get('cluster_id')
        x = pixels[index].get('x')
        y = pixels[index].get('y')
        r = int(clusters[cluster].get('R'))
        g = int(clusters[cluster].get('G'))
        b = int(clusters[cluster].get('B'))
        f.write('{0},{1},{2},{3},{4}\n'.format(x, y, r, g, b))

print model.summary()