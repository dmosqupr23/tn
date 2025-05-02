import mysql.connector
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

os.environ["JAVA_HOME"]
os.environ["SPARK_HOME"]
MYSQL_USER = os.environ['MYSQL_USER']
MYSQL_PASS = os.environ['MYSQL_PASS']
PATH_MYSQL_JAR = os.environ['PATH_MYSQL_JAR']

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("tn") \
    .config("spark.jars", PATH_MYSQL_JAR) \
    .getOrCreate()

current_date = datetime(2025, 4, 29).strftime("%Y%m%d")
#current_date = datetime.now().strftime("%Y%m%d")

fire_data_df = spark.read.csv(f"./Fire_Incidents_{current_date}.csv", header=True, inferSchema=True)

dimensions_columns = ['Primary Situation', 'Mutual Aid', 'Action Taken Primary', 'Action Taken Secondary', 'Action Taken Other', 'Detector Alerted Occupants', 'Property Use', 'Area of Fire Origin', 'Ignition Cause', 
                      'Ignition Factor Primary', 'Ignition Factor Secondary', 'Heat Source', 'Item First Ignited', 'Human Factors Associated with Ignition', 'Structure Type', 'Structure Status', 
                      'Fire Spread', 'Detectors Present', 'Detector Operation', 'Detector Effectiveness', 'Detector Failure Reason', 'Automatic Extinguishing System Present', 
                      'Automatic Extinguishing System Type', 'Automatic Extinguishing System Perfomance', 'Automatic Extinguishing System Failure Reason']

def clean_df(df):
    df = df.dropDuplicates()
    df = df \
    .withColumn('Automatic Extinguishing System Type', regexp_replace(col('Automatic Extinguishing Sytem Type'), '-', '')) \
    .withColumn('Detector Type', regexp_replace(col('Detector Type'), '-', '')) \
    .withColumn('Detector Operation', regexp_replace(col('Detector Operation'), '-', '')) \
    .withColumn('Detector Effectiveness', regexp_replace(col('Detector Effectiveness'), '-', '')) \
    .withColumn('Detector Failure Reason', regexp_replace(col('Detector Failure Reason'), '-', '')) \
    .withColumn('Automatic Extinguishing System Perfomance', regexp_replace(col('Automatic Extinguishing Sytem Perfomance'), '-', '')) \
    .withColumn('Automatic Extinguishing System Failure Reason', regexp_replace(col('Automatic Extinguishing Sytem Failure Reason'), '-', '')) \
    .withColumn('Automatic Extinguishing System Present', regexp_replace(col('Automatic Extinguishing System Present'), '-', '')) \
    .withColumn('Detectors Present', regexp_replace(col('Detectors Present'), '-', '')) \
    .withColumn('Fire Spread', regexp_replace(col('Fire Spread'), '-', '')) \
    .withColumn('Fire Spread', 
                when(col('Fire Spread').rlike(r'^00'), '00 Item first ignited, other')
                .when(col('Fire Spread').rlike(r'^11'), '11 Exterior roof covering or finish')
                .when(col('Fire Spread').rlike(r'^12'), '12 Exterior wall covering or finish')
                .when(col('Fire Spread').rlike(r'^15'), '15 Interior wall covering excluding drapes, etc.')
                .when(col('Fire Spread').rlike(r'^61'), '61 Atomized liquid, vaporized liquid, aerosol.')
                .when(col('Fire Spread').rlike(r'^66'), '66 Pipe, duct, conduit, hose')
                .when(col('Fire Spread').rlike(r'^UU'), 'U Undetermined')
                .otherwise(col('Fire Spread'))) \
    .withColumn('Fire Spread', regexp_replace(col('Fire Spread'), r'\.,', ',')) \
    .withColumn('Structure Status', regexp_replace(col('Structure Status'), '-', '')) \
    .withColumn('Structure Type', regexp_replace(col('Structure Type'), '-', '')) \
    .withColumn('Item First Ignited', 
                when(col('Item First Ignited').rlike(r'^00'), '00 Item first ignited, other')
                .when(col('Item First Ignited').rlike(r'^11'), '11 Exterior roof covering or finish')
                .when(col('Item First Ignited').rlike(r'^12'), '12 Exterior wall covering or finish')
                .when(col('Item First Ignited').rlike(r'^15'), '15 Interior wall covering excluding drapes, etc.')
                .when(col('Item First Ignited').rlike(r'^61'), '61 Atomized liquid, vaporized liquid, aerosol.')
                .when(col('Item First Ignited').rlike(r'^66'), '66 Pipe, duct, conduit, hose')
                .when(col('Item First Ignited').rlike(r'^UU'), 'U Undetermined')
                .otherwise(col('Item First Ignited'))) \
    .withColumn('Heat Source', regexp_replace(col('Heat Source'), '-', '')) \
    .withColumn('Heat Source', regexp_replace(col('Heat Source'), '  ', ' ')) \
    .withColumn('Heat Source', 
                when(col('Heat Source').rlike(r'^00'), '70 Chemical, natural heat source, other')
                .when(col('Heat Source').rlike(r'^11'), '11 Spark, ember, or flame from operating equipment')
                .when(col('Heat Source').rlike(r'^12'), '12 Radiated or conducted heat from operating equipment')
                .when(col('Heat Source').rlike(r'^13'), '13 Electrical arcing')
                .when(col('Heat Source').rlike(r'^40'), '40 Hot or smoldering object, other')
                .when(col('Heat Source').rlike(r'^41'), '41 Heat, spark from friction')
                .when(col('Heat Source').rlike(r'^42'), '42 Molten, hot material')
                .when(col('Heat Source').rlike(r'^43'), '43 Hot ember or ash')
                .when(col('Heat Source').rlike(r'^60'), '60 Heat from other open flame or smoking materials, other')
                .when(col('Heat Source').rlike(r'^61'), '61 Cigarette')
                .when(col('Heat Source').rlike(r'^63'), '63 Heat from undetermined smoking material')
                .when(col('Heat Source').rlike(r'^64'), '64 Match')
                .when(col('Heat Source').rlike(r'^65'), '65 Cigarette lighter')
                .when(col('Heat Source').rlike(r'^66'), '66 Candle')
                .when(col('Heat Source').rlike(r'^67'), '67 Warning or road flare; fuse')
                .when(col('Heat Source').rlike(r'^68'), '68 Backfire from internal combustion engine')
                .when(col('Heat Source').rlike(r'^70'), '70 Chemical, natural heat source, other')
                .when(col('Heat Source').rlike(r'^72'), '72 Spontaneous combustion, chemical reaction')
                .when(col('Heat Source').rlike(r'^80'), '80 Heat spread from another fire, other')
                .when(col('Heat Source').rlike(r'^81'), '81 Heat from direct flame, convection currents')
                .when(col('Heat Source').rlike(r'^97'), '97 Multiple heat sources including multiple ignitions')
                .when(col('Heat Source').rlike(r'^U '), 'U Undetermined')
                .when(col('Heat Source').rlike(r'^NN'), 'U Undetermined')
                .otherwise(col('Heat Source'))) \
    .withColumn('Ignition Factor Secondary', regexp_replace(col('Ignition Factor Secondary'), ' -', '')) \
    .withColumn('Ignition Factor Secondary', 
                when(col('Ignition Factor Secondary').rlike(r'^00'), '00 Other factor contributed to ignition')
                .when(col('Ignition Factor Secondary').rlike(r'^11'), '11 Abandoned or discarded materials or products')
                .when(col('Ignition Factor Secondary').rlike(r'^12'), '12 Heat source too close to combustibles')
                .when(col('Ignition Factor Secondary').rlike(r'^18'), '18 Improper container or storage procedure')
                .when(col('Ignition Factor Secondary').rlike(r'^32'), '32 Short circuit arc from mechanical damage')
                .when(col('Ignition Factor Secondary').rlike(r'^33'), '33 Shortcircuit arc from defective, worn insulation')
                .when(col('Ignition Factor Secondary').rlike(r'^52'), '52 Accidentally turned on, not turned off')
                .when(col('Ignition Factor Secondary').rlike(r'^57'), '57 Equipment not used for purpose intended')
                .when(col('Ignition Factor Secondary').rlike(r'^73'), '73 Outside/open fire for debris or waste disposal')
                .when(col('Ignition Factor Secondary').rlike(r'^74'), '74 Outside/open fire for warming or cooking')
                .otherwise(col('Ignition Factor Secondary'))) \
    .withColumn('Ignition Factor Primary', regexp_replace(col('Ignition Factor Primary'), '-', '')) \
    .withColumn('Ignition Factor Primary', regexp_replace(col('Ignition Factor Primary'), '  ', ' ')) \
    .withColumn('Ignition Factor Primary', 
                when(col('Ignition Factor Primary').rlike(r'^00'), '00 Other factor contributed to ignition')
                .when(col('Ignition Factor Primary').rlike(r'^11'), '11 Abandoned or discarded materials or products')
                .when(col('Ignition Factor Primary').rlike(r'^12'), '12 Heat source too close to combustibles')
                .when(col('Ignition Factor Primary').rlike(r'^13'), '13 Cutting, welding too close to combustible')
                .when(col('Ignition Factor Primary').rlike(r'^17'), '17 Washing part, painting with flammable liquid')
                .when(col('Ignition Factor Primary').rlike(r'^18'), '18 Improper container or storage procedure')
                .when(col('Ignition Factor Primary').rlike(r'^2'), 'U Undetermined')
                .when(col('Ignition Factor Primary').rlike(r'^31'), '31 Water caused shortcircuit arc')
                .when(col('Ignition Factor Primary').rlike(r'^32'), '32 Shortcircuit arc from mechanical damage')
                .when(col('Ignition Factor Primary').rlike(r'^33'), '33 Shortcircuit arc from defective, worn insulation')
                .when(col('Ignition Factor Primary').rlike(r'^35'), '35 Arc from faulty contact, broken conductor')
                .when(col('Ignition Factor Primary').rlike(r'^40'), '40 Design, manufacture, installation deficiency, other')
                .when(col('Ignition Factor Primary').rlike(r'^51'), '51 Collision, knock down, run over, turn over')
                .when(col('Ignition Factor Primary').rlike(r'^52'), '52 Accidentally turned on, not turned off')
                .when(col('Ignition Factor Primary').rlike(r'^57'), '57 Equipment not used for purpose intended')
                .when(col('Ignition Factor Primary').rlike(r'^58'), '58 Equipment not being operated properly')
                .when(col('Ignition Factor Primary').rlike(r'^5U'), '5U Undetermined Accidentally turned on, not turned off')
                .when(col('Ignition Factor Primary').rlike(r'^73'), '73 Outside/open fire for debris or waste disposal')
                .when(col('Ignition Factor Primary').rlike(r'^74'), '74 Outside/open fire for warming or cooking')
                .when(col('Ignition Factor Primary').rlike(r'^NN'), 'U Undetermined')
                .when(col('Ignition Factor Primary').rlike(r'^U '), 'U Undetermined')
                .when(col('Ignition Factor Primary').rlike(r'^UU'), 'U Undetermined')
                .when(col('Ignition Factor Primary').rlike(r'^7U'), '7U Undetermined Rekindle')
                .when(col('Ignition Factor Primary').rlike(r'^otherr'), 'other')
                .otherwise(col('Ignition Factor Primary'))) \
    .withColumn('Ignition Cause', regexp_replace(col('Ignition Cause'), ' -', '')) \
    .withColumn('Ignition Cause', 
                when(col('Ignition Cause').rlike(r'^0 Cause, other'), '0 Cause, other')
                .when(col('Ignition Cause').rlike(r'^U Cause undetermined after investigation'), 'U Undetermined')
                .otherwise(col('Ignition Cause'))) \
    .withColumn('Area of Fire Origin', regexp_replace(col('Area of Fire Origin'), ' -', '')) \
    .withColumn('Area of Fire Origin', 
                when(col('Area of Fire Origin').rlike(r'^00'), '00 Other area of fire origin')
                .when(col('Area of Fire Origin').rlike(r'^02'), '02 Exterior stairway, ramp, or fire escape')
                .when(col('Area of Fire Origin').rlike(r'^05'), '05 Entrance way, lobby')
                .when(col('Area of Fire Origin').rlike(r'^11'), '11 Arena, assembly area w/ fixed seats 100+ persons')
                .when(col('Area of Fire Origin').rlike(r'^12'), '12 Assembly area w/o fixed seats 100+')
                .when(col('Area of Fire Origin').rlike(r'^14'), '14 Common room, den, family room, living room, lounge')
                .when(col('Area of Fire Origin').rlike(r'^15'), '15 Sales area, showroom (excludes display window)')
                .when(col('Area of Fire Origin').rlike(r'^20'), '20 Function area, other')
                .when(col('Area of Fire Origin').rlike(r'^21'), '21 Bedroom < 5 persons; included are jail or prison')
                .when(col('Area of Fire Origin').rlike(r'^22'), '22 Bedroom 5+ persons; including barrack/dormitory')
                .when(col('Area of Fire Origin').rlike(r'^25'), '25 Bathroom, checkroom, lavatory, locker room')
                .when(col('Area of Fire Origin').rlike(r'^28'), '28 Personal service area, barber/beauty salon area')
                .when(col('Area of Fire Origin').rlike(r'^32'), '32 Dark room, photography area, or printing area')
                .when(col('Area of Fire Origin').rlike(r'^32'), '34 Surgery area major operations, operating room')
                .when(col('Area of Fire Origin').rlike(r'^38'), '38 Processing/manufacturing area, workroom')
                .when(col('Area of Fire Origin').rlike(r'^43'), '43 Storage: supplies or tools; dead storage')
                .when(col('Area of Fire Origin').rlike(r'^45'), '45 Shipping/receiving area; loading area, dock or bay')
                .when(col('Area of Fire Origin').rlike(r'^46'), '46 Chute/container trash, rubbish, waste')
                .when(col('Area of Fire Origin').rlike(r'^51'), '51 Dumbwaiter or elavator shaft')
                .when(col('Area of Fire Origin').rlike(r'^52'), '52 Conduit, pipe, utility, or ventilation shaft')
                .when(col('Area of Fire Origin').rlike(r'^54'), '54 Chute; laundry or mail, excluding trash chutes')
                .when(col('Area of Fire Origin').rlike(r'^55'), '55 Duct: hvac, cable, exhaust, heating, or AC')
                .when(col('Area of Fire Origin').rlike(r'^61'), '61 Machinery room or area; elevator machinery room')
                .when(col('Area of Fire Origin').rlike(r'^65'), '65 Maintenance shop or area, paint shop or area')
                .when(col('Area of Fire Origin').rlike(r'^71'), '71 Substructure area or space, crawl space')
                .when(col('Area of Fire Origin').rlike(r'^73'), '73 Ceiling and floor assembly, crawl space between stories')
                .when(col('Area of Fire Origin').rlike(r'^74'), '74 Attic: vacant, crawl space above top story')
                .when(col('Area of Fire Origin').rlike(r'^75'), '75 Wall assembly, concealed wall space')
                .when(col('Area of Fire Origin').rlike(r'^81'), '81 Operator/passenger area of transportation equipment')
                .when(col('Area of Fire Origin').rlike(r'^82'), '82 Cargo/trunk area all vehicles')
                .when(col('Area of Fire Origin').rlike(r'^85'), '85 Separate operator/control area of transportation equipment')
                .when(col('Area of Fire Origin').rlike(r'^91'), '91 Railroad right-of-way: on or near')
                .when(col('Area of Fire Origin').rlike(r'^92'), '92 Highway, parking lot, street: on or near')
                .when(col('Area of Fire Origin').rlike(r'^93'), '93 Courtyard, patio, porch, terrace')
                .when(col('Area of Fire Origin').rlike(r'^94'), '94 Open area, outside; included are farmland, field')
                .when(col('Area of Fire Origin').rlike(r'^U'), 'U Undetermined')
                .when(col('Area of Fire Origin') == '0', '00 Other area of fire origin')
                .otherwise(col('Area of Fire Origin'))) \
    .withColumn('Property Use', regexp_replace(col('Property Use'), ' -', '')) \
    .withColumn('Property Use', 
                when(col('Property Use').isin(['1', '3', '4191', '5', '7003', 'NNN None', 'UUU Undetermined']), 'U Undetermined')
                .when(col('Property Use').rlike(r'^110'), '110 Fixed use recreation places, other')
                .when(col('Property Use').rlike(r'^111'), '111 Bowling establishment')
                .when(col('Property Use').rlike(r'^120'), '120 Variable use amusement, recreation places')
                .when(col('Property Use').rlike(r'^130'), '130 Places of worship, funeral parlors')
                .when(col('Property Use').rlike(r'^131'), '131 Church, mosque, synagogue, temple, chapel')
                .when(col('Property Use').rlike(r'^154'), '154 Memorial structure, including monuments & statues')
                .when(col('Property Use').rlike(r'^160'), '160 Eating, drinking places')
                .when(col('Property Use').rlike(r'^182'), '182 Auditorium or concert hall')
                .when(col('Property Use').rlike(r'^210'), '210 Schools, non-adult')
                .when(col('Property Use').rlike(r'^213'), '213 Elementary school, including kindergarten')
                .when(col('Property Use').rlike(r'^215'), '215 Middle/Junior or High School')
                .when(col('Property Use').rlike(r'^241'), '241 Adult education center, college classroom')
                .when(col('Property Use').rlike(r'^300'), '300 Health care, detention, & correction, other')
                .when(col('Property Use').rlike(r'^311'), '311 24-hour care Nursing homes, 4 or more persons')
                .when(col('Property Use').rlike(r'^321'), '321 Mental retardation/development disability facility')
                .when(col('Property Use').rlike(r'^322'), '322 Alcohol or substance abuse recovery center')
                .when(col('Property Use').rlike(r'^340'), '340 Clinics, doctors offices, hemodialysis cntr, other')
                .when(col('Property Use').rlike(r'^342'), "342 Doctor, dentist or oral surgeon's office")
                .when(col('Property Use').rlike(r'^429'), '429 Multifamily dwellings')
                .when(col('Property Use').rlike(r'^439'), '439 Boarding/rooming house, residential hotels')
                .when(col('Property Use').rlike(r'^460'), '460 Dormitory type residence, other')
                .when(col('Property Use').rlike(r'^557'), '557 Personal service, including barber & beauty shops')
                .when(col('Property Use').rlike(r'^559'), '559 Recreational, hobby, home repair sales, pet store')
                .when(col('Property Use').rlike(r'^579'), '579 Motor vehicle or boat sales, services, repair')
                .when(col('Property Use').isin(['600 Ind., utility, defense, agriculture, mining, other', '600 Utility/Defense/Agriculture/mining, oth.']), '600 Ind. utility, defense, agriculture, mining, other')
                .when(col('Property Use').rlike(r'^614'), '614 Steam or heat generating plant')
                .when(col('Property Use').rlike(r'^629'), '629 Laboratory or science laboratory')
                .when(col('Property Use').rlike(r'^644'), '644 Gas distribution, gas pipeline')
                .when(col('Property Use').rlike(r'^645'), '645 Flammable liquid distribution, F.L. pipeline')
                .when(col('Property Use').rlike(r'^881'), '881 Parking garage, (detached residential garage)')
                .when(col('Property Use').rlike(r'^899'), '899 Residential or self storage units')
                .when(col('Property Use').rlike(r'^951'), '951 Railroad right-of-way')
                .when(col('Property Use').rlike(r'^961'), '961 Highway or divided highway (Street)')
                .when(col('Property Use').rlike(r'^961'), '962 Residential street, road or residential driveway')
                .when(col('Property Use').rlike(r'^973'), '973 Aircraft taxi-way')
                .when(col('Property Use').rlike(r'^983'), '983 Pipeline, power line or other utility right-of-way')
                .otherwise(col('Property Use'))) \
    .withColumn('Detector Alerted Occupants', regexp_replace(col('Detector Alerted Occupants'), ' -', '')) \
    .withColumn('Detector Alerted Occupants', when(col('Detector Alerted Occupants').rlike(r'^U'), 'U Undetermined').otherwise(col('Detector Alerted Occupants'))) \
    .withColumn('Action Taken Other', regexp_replace(col('Action Taken Other'), ' -', '')) \
    .withColumn('Action Taken Other', 
                when(col('Action Taken Other').rlike(r'^11'), '11 Extinguishment by fire service personnel')
                .when(col('Action Taken Other').rlike(r'^43'), '43 Hazardous materials spill control and confinement')
                .when(col('Action Taken Other').rlike(r'^44'), '44 Hazardous materials leak control & containment')
                .when(col('Action Taken Other').rlike(r'^50'), '50 Fires, rescues & hazardous conditions, other')
                .when(col('Action Taken Other').rlike(r'^62'), '62 Restore sprinkler or fire protection system')
                .when(col('Action Taken Other').rlike(r'^80'), '80 Information, investigation & enforcement, other')
                .when(col('Action Taken Other').rlike(r'^83'), '83 Provide information to public or media')
                .when(col('Action Taken Other').rlike(r'^87'), '87 Investigate fire out on arrival')
                .otherwise(col('Action Taken Other'))) \
    .withColumn('Action Taken Secondary', regexp_replace(col('Action Taken Secondary'), ' -', '')) \
    .withColumn('Action Taken Secondary', 
                when(col('Action Taken Secondary').rlike(r'^10'), '10 Fire control or extinguishment, other')
                .when(col('Action Taken Secondary').rlike(r'^11'), '11 Extinguishment by fire service personnel')
                .when(col('Action Taken Secondary').rlike(r'^42'), '42 HazMat detection, monitoring, sampling, & analysis')
                .when(col('Action Taken Secondary').rlike(r'^44'), '44 Hazardous materials leak control & containment')
                .when(col('Action Taken Secondary').rlike(r'^62'), '62 Restore sprinkler or fire protection system')
                .when(col('Action Taken Secondary').rlike(r'^79'), '79 Assess severe weather or natural disaster damage')
                .when(col('Action Taken Secondary').rlike(r'^80'), '80 Information, investigation & enforcement, other')
                .when(col('Action Taken Secondary').rlike(r'^83'), '83 Provide information to public or media')
                .when(col('Action Taken Secondary').rlike(r'^85'), '85 Enforce code')
                .when(col('Action Taken Secondary').rlike(r'^87'), '87 Investigate fire out on arrival')
                .when(col('Action Taken Secondary').rlike(r'^93'), '93 Cancelled enroute')
                .otherwise(col('Action Taken Secondary'))) \
    .withColumn('Action Taken Primary', regexp_replace(col('Action Taken Primary'), ' -', '')) \
    .withColumn('Action Taken Primary', 
                when(col('Action Taken Primary').isin(['1', '112', '8']), 'U Undetermined')
                .when(col('Action Taken Primary').rlike(r'^10'), '10 Fire control or extinguishment, other')
                .when(col('Action Taken Primary').rlike(r'^11'), '11 Extinguishment by fire service personnel ')
                .when(col('Action Taken Primary').rlike(r'^42'), '42 HazMat detection, monitoring, sampling, & analysis')
                .when(col('Action Taken Primary').rlike(r'^43'), '43 Hazardous materials spill control and confinement')
                .when(col('Action Taken Primary').rlike(r'^44'), '44 Hazardous materials leak control & containment')
                .when(col('Action Taken Primary').rlike(r'^50'), '50 Fires, rescues & hazardous conditions, other')
                .when(col('Action Taken Primary').rlike(r'^62'), '62 Restore sprinkler or fire protection system')
                .when(col('Action Taken Primary').rlike(r'^79'), '79 Assess severe weather or natural disaster damage')
                .when(col('Action Taken Primary').rlike(r'^80'), '80 Information, investigation & enforcement, other')
                .when(col('Action Taken Primary').rlike(r'^85'), '85 Enforce code')
                .when(col('Action Taken Primary').rlike(r'^87'), '87 Investigate fire out on arrival')
                .when(col('Action Taken Primary').rlike(r'^93'), '93 Cancelled enroute')
                .otherwise(col('Action Taken Primary'))) \
    .withColumn('Mutual Aid', 
                when(col('Mutual Aid') == 'Automatic aid given', '4 Automatic aid given')
                .when(col('Mutual Aid') == 'Mutual aid given', '3 Mutual aid given')
                .when(col('Mutual Aid') == 'Mutual aid received', '2 Automatic aid received')
                .when(col('Mutual Aid').rlike(r'^N'), 'U Undetermined')
                .when(col('Mutual Aid') == 'Automatic or contract aid received', '2 Automatic aid received')
                .when(col('Mutual Aid') == 'Other aid given', '5 Other aid given')
                .otherwise(col('Mutual Aid'))) \
    .withColumn('Primary Situation', regexp_replace(col('Primary Situation'), ' -', '')) \
    .withColumn('Primary Situation', 
                when(col('Primary Situation').isin(['1', '10', '11', '25*', '71', '75', 'CR', 'Y', '']), 'U Undetermined')
                .when(col('Primary Situation').rlike(r'^112'), '112 Fires in structure other than in a building')
                .when(col('Primary Situation').rlike(r'^114'), '114 Chimney or flue fire, confined to chimney or flue')
                .when(col('Primary Situation').rlike(r'^115'), '115 Incinerator overload or malfunction, fire confined')
                .when(col('Primary Situation').rlike(r'^116'), '116 Fuel burner/boiler malfunction, fire confined')
                .when(col('Primary Situation').rlike(r'^117'), '117 Commercial Compactor fire, confined to rubbish')
                .when(col('Primary Situation').rlike(r'^120'), '120 Fire in mobile prop. used as a fixed struc., other')
                .when(col('Primary Situation').rlike(r'^121'), '121 Fire in mobile home used as fixed residence ')
                .when(col('Primary Situation').rlike(r'^122'), '122 Fire in motor home, camper, recreational vehicle')
                .when(col('Primary Situation').rlike(r'^123'), '123 Fire in portable building, fixed location')
                .when(col('Primary Situation').rlike(r'^134'), '134 Water vehicle fire (Boat, Ship, etc.)')
                .when(col('Primary Situation').rlike(r'^136'), '136 Self-propelled motor home or recreational vehicle')
                .when(col('Primary Situation').rlike(r'^142'), '142 Brush, or brush and grass mixture fire')
                .when(col('Primary Situation').rlike(r'^154'), '154 Dumpster or other outside trash receptacle fire')
                .when(col('Primary Situation').rlike(r'^155'), '155 Outside stationary compactor/compacted trash fire')
                .when(col('Primary Situation').rlike(r'^160'), '160 Special outside fire, other')
                .when(col('Primary Situation').rlike(r'^162'), '162 Outside equipment fire')
                .when(col('Primary Situation').rlike(r'^163'), '163 Outside gas or vapor combustion explosion')
                .when(col('Primary Situation').rlike(r'^200'), '200 Overpressure rupture, explosion, overheat other')
                .when(col('Primary Situation').rlike(r'^210'), '210 Overpressure rupture from steam, other')
                .when(col('Primary Situation').rlike(r'^211'), '211 Overpressure rupture of steam pipe or pipeline')
                .when(col('Primary Situation').rlike(r'^213'), '213 Steam rupture of pressure or process vessel')
                .when(col('Primary Situation').rlike(r'^220'), '220 Overpressure rupture from air or gas, other')
                .when(col('Primary Situation').rlike(r'^221'), '221 Overpressure rupture of air or gas pipe/pipeline')
                .when(col('Primary Situation').rlike(r'^222'), '222 Overpressure rupture of boiler from air or gas')
                .when(col('Primary Situation').rlike(r'^223'), '223 Air or gas rupture of pressure or process vessel')
                .when(col('Primary Situation').rlike(r'^231'), '231 Chemical reaction rupture of process vessel')
                .when(col('Primary Situation').rlike(r'^244'), '244 Dust explosion (no fire)')
                .when(col('Primary Situation').rlike(r'^251'), '251 Excessive heat, scorch burns with no ignition')
                .when(col('Primary Situation').rlike(r'^300'), '300 Rescue, EMS incident, other')
                .when(col('Primary Situation').rlike(r'^320'), '320 Emergency medical service incident, other')
                .when(col('Primary Situation').rlike(r'^321'), '321 EMS call, excluding vehicle accident with injury')
                .when(col('Primary Situation').rlike(r'^322'), '322 Motor vehicle accident with injuries')
                .when(col('Primary Situation').rlike(r'^323'), '323 Motor vehicle/pedestrian accident (MV Ped)')
                .when(col('Primary Situation').rlike(r'^340'), '340 Search for lost person, other')
                .when(col('Primary Situation').rlike(r'^351'), '351 Extrication of victim(s) from building/structure')
                .when(col('Primary Situation').rlike(r'^353'), '353 Removal of victim(s) from stalled elevator')
                .when(col('Primary Situation').rlike(r'^354'), '354 Trench/below-grade rescue')
                .when(col('Primary Situation').rlike(r'^356'), '356 High-angle rescue')
                .when(col('Primary Situation').rlike(r'^360'), '360 Water & ice related rescue, other')
                .when(col('Primary Situation').rlike(r'^400'), '400 Hazardous condition, other')
                .when(col('Primary Situation').rlike(r'^410'), '410 Flammable gas or liquid condition, other')
                .when(col('Primary Situation').rlike(r'^420'), '420 Toxic condition, other')
                .when(col('Primary Situation').rlike(r'^422'), '422 Chemical spill or leak')
                .when(col('Primary Situation').rlike(r'^440'), '440 Electrical wiring/equipment problem, other')
                .when(col('Primary Situation').rlike(r'^441'), '441 Heat from short circuit (wiring), defective/worn')
                .when(col('Primary Situation').rlike(r'^444'), '444 Power line down (wire)')
                .when(col('Primary Situation').rlike(r'^445'), '445 Arcing, shorted electrical equipment')
                .when(col('Primary Situation').rlike(r'^461'), '461 Building or structure weakened or collapsed')
                .when(col('Primary Situation').rlike(r'^471'), '471 Explosive, bomb removal (for bomb scare, use 721)')
                .when(col('Primary Situation').rlike(r'^551'), '551 Assist police or other governmental agency')
                .when(col('Primary Situation').rlike(r'^622'), '622 No incident found on arrival at dispatch address')
                .when(col('Primary Situation').rlike(r'^641'), '641 Vicinity alarm (incident in other location)')
                .when(col('Primary Situation').rlike(r'^650'), '650 Steam, other gas mistaken for smoke, other')
                .when(col('Primary Situation').rlike(r'^652'), '652 Steam, vapor, fog or dust thought to be smoke')
                .when(col('Primary Situation').rlike(r'^653'), '653 Smoke from barbecue, tar kettle ')
                .when(col('Primary Situation').rlike(r'^661'), '661 EMS call, party transported by non-fire agency')
                .when(col('Primary Situation').rlike(r'^671'), '671 Hazmat release investigation w/no hazmat')
                .when(col('Primary Situation').rlike(r'^672'), '672 Biological hazard investigation, none found')
                .when(col('Primary Situation').rlike(r'^711'), '711 Municipal alarm system, malicious false alarm')
                .when(col('Primary Situation').rlike(r'^712'), '712 Direct tie to FD, malicious false alarm')
                .when(col('Primary Situation').rlike(r'^715'), '715 Local alarm system, malicious false alarm')
                .when(col('Primary Situation').rlike(r'^732'), '732 Extinguishing system activation due to malfunction')
                .when(col('Primary Situation').rlike(r'^733'), '733 Smoke detector activation due to malfunction')
                .when(col('Primary Situation').rlike(r'^734'), '734 Heat detector activation due to malfunction')
                .when(col('Primary Situation').rlike(r'^736'), '736 CO detector activation due to malfunction')
                .when(col('Primary Situation').rlike(r'^740'), '740 Unintentional transmission of alarm, other')
                .when(col('Primary Situation').rlike(r'^741'), '741 Sprinkler activation, no fire unintentional')
                .when(col('Primary Situation').rlike(r'^743'), '743 Smoke detector activation, no fire unintentional')
                .when(col('Primary Situation').rlike(r'^744'), '744 Detector activation, no fire unintentional')
                .when(col('Primary Situation').rlike(r'^745'), '745 Alarm system activation, no fire unintentional')
                .when(col('Primary Situation').rlike(r'^746'), '746 Carbon monoxide detector activation, no CO')
                .when(col('Primary Situation').rlike(r'^800'), '800 Severe weather or natural disaster, other')
                .when(col('Primary Situation').rlike(r'^815'), '815 Severe weather or natural disaster standby')
                .otherwise(col('Primary Situation'))) \
    .withColumn('Estimated Contents Loss',
                when(col('Estimated Contents Loss').isNull(), 0)
                .when(col('Estimated Contents Loss') < 0, 0)
                .otherwise(col('Estimated Contents Loss'))) \
    .withColumn('Estimated Property Loss',
                when(col('Estimated Property Loss').isNull(), 0)
                .when(col('Estimated Property Loss') < 0, 0)
                .otherwise(col('Estimated Property Loss'))) \
    .withColumn('City', upper('City')) \
    .withColumn('City', 
                when(col('City').isin(['BRISBANE']), 'BN')
                .when(col('City').isin(['DALY CITY']), 'DC')
                .when(col('City').isin(['FORT MASON']), 'FM')
                .when(col('City').isin(['HUNTERS POINT']), 'HP')
                .when(col('City').isin(['PRESIDIO']), 'PR')
                .when(col('City').isin(['SAN FRANCISCO', 'SFO']), 'SF')
                .when(col('City').isin(['TREASURE ISLA', 'TREASURE ISLAND']), 'TI')
                .when(col('City').isin(['YERBA BUENA']), 'YB')
                .otherwise(col('City'))) \
    .na.fill(value='U Undetermined')
    return df

def normalize_colums(df):
    for col_name in df.columns:
        new_col_name = col_name.lower().replace(' ', '_')
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

def split_property_column(df, column_name):
    id_column = f"id"
    desc_column = f"description"

    df_result = df.select(column_name).distinct().orderBy(column_name) \
                  .withColumn(id_column, regexp_extract(col(column_name), r'^([^\s]+)', 1)) \
                  .withColumn(desc_column, regexp_extract(col(column_name), r'^[^\s]+\s+(.*)', 1)) \
                  .select(id_column, desc_column)

    return df_result

def create_table_mysql(table_name):
    table_name = table_name.replace(' ', '_').lower()+'_dimension'
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id VARCHAR(5),
        description VARCHAR(255),
        PRIMARY KEY (id)
    );
    """
    conexion = mysql.connector.connect(
    host="localhost", 
    user = MYSQL_USER,
    password = MYSQL_PASS,
    database="tn"
    )
    cursor = conexion.cursor()
    cursor.execute(create_table_query)
    conexion.commit()
    print(f"Tabla {table_name} creada exitosamente.")

#for col in dimensions_columns:
#    create_table_mysql(col)

def insert_into_mysql(df, table_name, mysql_url, user, password, mode='append'):
    df.write \
      .format("jdbc") \
      .option("url", mysql_url) \
      .option("driver", "com.mysql.cj.jdbc.Driver") \
      .option("dbtable", table_name) \
      .option("user", user) \
      .option("password", password) \
      .mode(mode) \
      .save()

fire_data_df = normalize_colums(clean_df(fire_data_df))


for col_name in dimensions_columns:
    df_insert = split_property_column(fire_data_df.select(col_name), col_name)
    print(col_name)
    
    insert_into_mysql(
        df=df_insert,
        table_name=col_name.replace(' ', '_').lower()+'_dimension',
        mysql_url="jdbc:mysql://localhost:3306/tn",
        user=MYSQL_USER,
        password=MYSQL_PASS,
        mode="overwrite"
    )
    print(f"Datos insertados en la tabla {col_name.replace(' ', '_').lower()+'_dimension'} exitosamente.")

insert_into_mysql(fire_data_df, "fire_incidents_desnormalized", mysql_url="jdbc:mysql://localhost:3306/tn", user=MYSQL_USER, password=MYSQL_PASS, mode="overwrite")
fire_data_df.show()
