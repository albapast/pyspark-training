from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    '''
    Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
    and output the airport's name and the city's name to out/airports_in_usa.text.

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:
    "Putnam County Airport", "Greencastle"
    "Dowagiac Municipal Airport", "Dowagiac"
    ...
    '''
conf = SparkConf().setAppName("airport_problem").setMaster("local[*]")
sc = SparkContext(conf=conf)

airports_rdd = sc.textFile("in/airports.text")
airports_eeuu = airports_rdd.filter(lambda line: line.split(",")[3] == "\"United States\"")
airports_output = airports_eeuu.map(lambda line: "{}, {}".format(line.split(",")[1],line.split(",")[2]))
airports_output.saveAsTextFile("out/airports_usa_me.txt")
