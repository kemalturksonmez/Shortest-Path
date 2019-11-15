import geojsonio
import json
import ast
from geopy.distance import geodesic
import math
from pymongo import MongoClient
from neo4j import GraphDatabase
from dataclasses import dataclass
from random import uniform
import random

#Data struct used to represent a point
@dataclass
class Point:
    long: float
    lat: float

#Maximum distance between two points       
MAXDIST = 15
#Maximum distance squared and floored
MAXDSQRD = math.floor(math.sqrt((MAXDIST**2)/2))
#Radius of earth
r_earth = 6378100

# Bounds of washington, the four corners of the square    
washPoints = [Point(-77.04101979732513,38.99586514404603),
                  Point( -77.17230319976807,38.89325198967832),
                  Point(-77.03887939453125,38.79182417693575),
                  Point(-76.90938234329224,38.892886657138156)]

# This method recieves data stored in a geojson manner and creates a gist with it
def sendToGeojson(data):
    data = json.dumps(data, indent=4)
    geoJ = json.loads(data)
    data = ast.literal_eval(geoJ)
    geoJ = json.dumps(data, indent=4)
    geojsonio.display(geoJ)



#Changes latitude by some amount of specified meters
def newLat(point, dx):
    newLat  = point.lat + (dx / r_earth) * (180 / math.pi)
    return newLat

#Changes longitute by some amount of specified meters
def newLong(point, dy):
    newLong = point.long + (dy / r_earth) * (180 / math.pi) / math.cos(point.lat * math.pi/180)
    return newLong

#returns the distance between two coordinates
def getDist(pt1, pt2):
    origin = (pt1.lat, pt1.long)
    dest = (pt2.lat, pt2.long)
    return geodesic(origin, dest).meters

#Returns output in geo style
def outGeo(points):
	coords = ""
	if len(points) == 1:
		inp = '"Point"'
		coords = str(points[0].long) + "," + str(points[0].lat)	
	else:
		inp = '"LineString"'
		for i in range(0, len(points)):
			temp = ("[" + str(points[i].long) + "," + str(points[i].lat) + "]")
			if i > 0:
				coords = coords + ", " + temp
			else:
				coords = temp
        
	out =(',{"type": "Feature", "properties": {},'
	+  '"geometry": { "type":' + inp + ', "coordinates": ['
	+   coords + ']}}')
	return out

# Connects to local mongo and returns reference to database
def connectToMongo():
    client = MongoClient('127.0.0.1',port=27017)
    db = client.mydb
    return db

#Returns all everything from mongo
def mongoGetAll(db):
    qr = db.washGeo.find({},{'_id': 0})
    data = ""
    for i in qr:
        data = data + str(i)
    return data

#Queries to see if any shapes intersect a point
# Returns false if there is an intersection
# Returns true if there is no intersection
def geoInterPoint(db, point):
    qr = db.washGeo.find_one({"features.geometry":
                             {"$geoIntersects": { "$geometry":
                                                 { "type": "Point",
                                                  "coordinates": [point.long, point.lat]}}}},
                            {"features.properties.NAME":1})
    return qr is None

#Queries to see if any shapes intersect a line
# Returns false if there is an intersection
# Returns true if there is no intersection
def geoInterLine(db, pt1, pt2):
    qr = db.washGeo.find_one({"features.geometry":
                             {"$geoIntersects": { "$geometry":
                                                 { "type": "LineString",
                                                  "coordinates": [[pt1.long, pt1.lat],[pt2.long, pt2.lat]]}}}},
                            {"features.properties.NAME":1})
    return qr is None


#Connects to a neo4j database
def connectToNeo4j():
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4j"))
    return driver

# Creates point in neo4j
def createPoint(tx, point):
    inp = ("CREATE (p:POINT {longitude: " + str(point.long) + ", latitude:" + str(point.lat) + "}) RETURN p")
    tx.run(inp)

# Creates a relation between two points assuming nothing is intersected between the two
def createRelation(tx, pt1, pt2):
    inp = ("MATCH (p1:POINT {longitude: " + str(pt1.long) 
    + ", latitude:" + str(pt1.lat) + "}), (p2:POINT {longitude: " 
    + str(pt2.long) + ", latitude:" + str(pt2.lat)  + "}) " 
    + "WITH p1,p2 "
    + "CREATE (p1)-[r:OPEN]->(p2) RETURN r")
    tx.run(inp)

# Queries graph database to see if points within some defined distance exists
# Queries for points that it knows exist within the database
# Returns pointer with query output
# If query was succesful pointer is iterable
# otherwise, empty
def matchNeighbors(tx, point):
    inp = ("MATCH (p1:POINT{longitude:" + str(point.long) + ", latitude:" + str(point.lat) +"}),"
    + "(p2:POINT) WITH distance(point(p1), point(p2)) as d, p2 "
    + "WHERE d < " + str(MAXDIST) + " AND p2 <> p1 AND NOT (p1)-[]-(p2) "
    + "RETURN p2.longitude, p2.latitude")
    return tx.run(inp)

# Queries graph database to see if points within some defined distance exists
# Queries neighbors for points that may not exist in database
# Returns pointer with query output
# If query was succesful pointer is iterable
# otherwise, empty
def matchNeighborsBeforeCreation(tx, point):
    inp = ("MATCH (p2:POINT) WITH distance(point(({longitude: " + str(point.long)
           + ", latitude: " + str(point.lat) + "})), point(p2)) as d, p2 " 
           + "WHERE d < 8 RETURN p2.longitude, p2.latitude")
    return tx.run(inp)


# Queries for shortest path between two nodes
# Returns pointer with query output
# If query was succesful pointer is iterable
# otherwise, empty
def matchShortestPath(tx, pt1, pt2):
    inp = ("MATCH (p1:POINT {longitude: " + str(pt1.long)
           +", latitude: " + str(pt1.lat) + "}) "
           + "MATCH (p2:POINT {longitude: " + str(pt2.long)
           +", latitude: " + str(pt2.lat) +"}) "
           + "MATCH path=shortestPath((p1)-[*]-(p2)) "
           + "RETURN extract(long IN nodes(path)| long.longitude) "
           + "AS longitude, extract(lat IN nodes(path)| lat.latitude) AS latitude")
    return tx.run(inp)

# Checks to see if point exists
# Returns pointer with query output
# If query was succesful pointer is iterable
# otherwise, empty
def matchPoint(tx, point):
    inp = ("MATCH (p:POINT{longitude:" + str(point.long)
           + ", latitude:" + str(point.lat) 
           + "}) RETURN p.longitude, p.latitude")
    return tx.run(inp)


# Find a random point inside washington
# return the random point
def randomPoint():
    long = uniform(washPoints[1].long,washPoints[3].long)
    lat = uniform(washPoints[2].lat,washPoints[0].lat)
    point = Point(long,lat)
    return point

# Checks to see if point is contained inside washington
# Returns true if point is in washington
# False otherwise
def ptContained(point):
    if (point.long < washPoints[3].long and 
        point.long > washPoints[1].long and 
        point.lat < washPoints[0].lat and 
        point.lat > washPoints[2].lat):
        return True
    else:
        return False
      
# Converts a point from neo4j into Point class output
# returns the point
def neo4jToPoint(out):
    return Point(out[0],out[1])

# Attempts to create relations between two point
# assuming there is no intersection of a building between the two
def createNeighborRelations(driver, db, point):
    with driver.session() as session:
        out = session.read_transaction(matchNeighbors, point)
        for i in out:
            #Create temporary point
            ptTemp = neo4jToPoint(i)
            # Checks to see if the relationship interesects a building
            if geoInterLine(db, point, ptTemp):
                # Creates relation if there is no intersection
                session.write_transaction(createRelation, point, ptTemp)
                print("relation created for point")
                
#Attempts to find a point with no interesection
def findNewPoint(db, point):
    ptTemp = point
    counter = 0
    #Checks new point for intersections and sees if it is within bounds
    while not geoInterPoint(db, ptTemp):
        a = random.randint(-MAXDSQRD, MAXDSQRD)
        b = random.randint(-MAXDSQRD, MAXDSQRD)
        ptTemp = Point(newLong(ptTemp,a),newLat(ptTemp,b))
        # Ensures point is within defined bounds of washington
        if not ptContained(ptTemp):
            print("Point was not inside Washington D.C.")
            return None
    print("New Point found")
    return ptTemp


#Checks for points near the given point
# If something exists, it'll return the first point
# If nothing exists, it'll make a point after checking for collisions
# return point
def createPointCombo(driver, db, point):
    cond = True
    with driver.session() as session:
        out = session.read_transaction(matchPoint, point)
        for i in out:
            #If it entered, it means the point exists
            cond = False   
        #If point doesn't exist check for neighbors
        if cond:
            out = session.read_transaction(matchNeighbors, point)
            #If nothing is returned, loop isn't entered
            for i in out:
                point = neo4jToPoint(i)
                cond = False         
        #Creates new point if there was no point near
        if cond:
            # If there is an intersection, attempts to find a new point
            if not geoInterPoint(db, point):
                #Attempts to find a new point
                point = findNewPoint(db,point)
            if point is None:
                return None
            out = session.read_transaction(matchPoint, point)
            for i in out:
                #point exists so return it
                return point
            out = session.read_transaction(matchNeighborsBeforeCreation, point)
           #looking for neighbors
            for i in out:
                point = neo4jToPoint(i)
                print("Using neighbor point! ", point)
                return point
            session.write_transaction(createPoint,point)
            print("Created new point", point)
            createNeighborRelations(driver, db, point)
            # Return either new point or new neighbor point
            return point
        # Return either original or neighbor point
        return point

# Populates graph database with random points
def populateNeo4j(driver, db):
    tmpPoint = 0
    while tmpPoint is not None:
        #Returns random point
        tmpPoint = randomPoint()
        #Attempts to find neighboring point or create a new point
        tmpPoint = createPointCombo(driver, db, tmpPoint)

# Creates path from one point to another
def createPath(driver, pt1, pt2):
    lat = 0
    long = 0
    if pt1.lat<pt2.lat:
        lat = newLat(pt1, MAXDSQRD)
    else:
        lat = newLat(pt1, -MAXDSQRD)
    if pt1.long<pt2.long:
        long = newLong(pt1, MAXDSQRD)
    else:
        long = newLong(pt1, -MAXDSQRD)
    return Point(long, lat)

# Translates output of neo4j into an array of points
def parseShortestPath(inp):
	inp = inp.split("=")
	longitude = inp[1]
	latitude = inp[2]

	longitude = longitude.split("[")
	longitude = longitude[1]
	longitude = longitude.split("]")
	longitude = longitude[0]
	longitude = longitude.split(",")

	latitude = latitude.split("[")
	latitude = latitude[1]
	latitude = latitude.split("]")
	latitude = latitude[0]
	latitude = latitude.split(",")

	points = []
	for i in range(0,len(latitude)):
		points.append(Point(float(longitude[i]), float(latitude[i])))
	return points




# Attempts to build path from point 1 to point 2
def shortestPath(driver, db, pt1, pt2):
    #Finds points that are point1 and point2 or near them in the neo4j graph
    pt1 = createPointCombo(driver, db, pt1)
    pt2 = createPointCombo(driver, db, pt2)
    cond = True
    with driver.session() as session:
        #If shortest path exists, dont attempt to set up path
        out = session.read_transaction(matchShortestPath, pt1,pt2)
        points = []
        for i in out:
            if cond:
                print("Shortest path found!")
                cond = False
            points = parseShortestPath(str(i))
        if cond:
            print("Shortest path doesn't exist, building new path")
            tmpPoint = pt1
            print("Building first path")
            while tmpPoint is not None and getDist(tmpPoint,pt2)>MAXDIST:
                tmpPoint = createPath(driver, tmpPoint, pt2)
                tmpPoint = createPointCombo(driver, db, tmpPoint)
            print("First path completed")
            tmpPoint = pt2 
            print("Building second path")
            while tmpPoint is not None and getDist(tmpPoint,pt1)>MAXDIST:
                tmpPoint = createPath(driver, tmpPoint, pt1)
                tmpPoint = createPointCombo(driver, db, tmpPoint)
            print("Second path completed")
            out = session.read_transaction(matchShortestPath, pt1,pt2)            
            for i in out:
                if cond:
                    print("Shortest path found!")
                    cond = False
                points = parseShortestPath(str(i))
            if cond:
                    print("No Shortest path found")
        return points

def buildNeo4j():
	db = connectToMongo()
	driver = connectToNeo4j()
	while True:
		shortestPath(driver,db,randomPoint(),randomPoint())

#Inserts in to the middle of a string
def insert_str(string, str_to_insert, index):
    return string[:index] + str_to_insert + string[index:]

def runProgram(point1, point2):
	db = connectToMongo()
	driver = connectToNeo4j()
	data = mongoGetAll(db)
	sendToGeojson(data)
	query = shortestPath(driver, db, point1, point2)
	# formats linestrings into geojson format and inserts data into the document
	out = outGeo(query)
	data = insert_str(data, out, len(data)-2)
	sendToGeojson(data)

print("Would you like to build the graph or find a path?")
userIn = input('Enter "build" or "path": ')
if userIn == "build":
	buildNeo4j()
elif userIn == "path":
	print("Latitude should be between -90 and 90 degrees")
	print("Longitude should be between -180 and 180 degrees")
	lat1, lat2, long1, long2 = 200, 200, 200, 200
	while long1 < -180 or long1 > 180:
		long1 = float(input("Enter the first longitude coordinate: "))
	while lat1 < -90 or lat1 > 90:
		lat1 = float(input("Enter the first latitude coordinate: "))
	while long2 < -180 or long2 > 180:
		long2 = float(input("Enter the first longitude coordinate: "))
	while lat2 < -90 or lat2 > 90:
		lat2 = float(input("Enter the second latitude coordinate: "))
	point1 = Point(long1, lat1)
	point2 = Point(long2, lat2)
	runProgram(point1,point2)

