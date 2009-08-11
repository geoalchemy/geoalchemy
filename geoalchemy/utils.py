
linestring_types = ("LINESTRING", "LineString", "ST_LineString")
polygon_types = ("POLYGON", "Polygon", "ST_Polygon")

def linestring_coordinates(wkt):
    wkt = wkt.split("(")[1]
    wkt = wkt.split(")")[0]
    wkt = wkt.strip()
    points = wkt.split(",")
    return tuple([tuple([float(value) for value in point.split(" ")]
                  ) for point in points])

#TODO Enhance it to support polygons with holes
def polygon_coordinates(wkt):
    wkt = "(" + "".join(wkt.split("(")[1:])
    wkt = "".join(wkt.split(")")[:-1]) + ")"
    return tuple([l for l in linestring_coordinates(wkt)])

def extract_wkt_coordinates(wkt, geom_type):
    if geom_type in linestring_types: return linestring_coordinates(wkt)
    elif geom_type in polygon_types: return polygon_coordinates(wkt)
    else: raise NotImplementedError("Not implemented for this geometry type")
