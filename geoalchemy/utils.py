
linestring_types = ("LineString", "ST_LineString")
polygon_types = ("Polygon", "ST_Polygon")

def linestring_coordinates(wkt):
    wkt = wkt.split("(")[1]
    wkt = wkt.split(")")[0]
    wkt = wkt.strip()
    points = wkt.split(",")
    coords = "("
    for point in points:
        c = point.split(" ")
        print c
        print ""
        coords += "(%s,%s)" % (c[0], c[1])
    return coords+")"

#TODO Enhance it to support polygons with holes
def polygon_coordinates(wkt):
    wkt = "(" + "".join(wkt.split("(")[1:])
    wkt = "".join(wkt.split(")")[:-1]) + ")"
    return linestring_coordinates(wkt)

def extract_coordinates(wkt, geom_type):
    if geom_type in linestring_types: return linestring_coordinates(wkt)
    elif geom_type in polygon_types: return polygon_coordinates(wkt)
    else: raise NotImplementedError("Not implemented for this geometry type")
