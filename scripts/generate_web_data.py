from random import randint
from random import choice

file = open("/home/acadgild/project/data/web/file.xml", "w")
count = 20

file.write("<records>\n")

while (count > 0):
	geo_cd_list=["A", "E", "AU", "AP", "U"]
	song_end_type_list=["0","1","2","3"]
	timestamp_list=["2016-05-10 12:24:22", "2016-06-09 22:12:36", "2016-07-10 01:38:09", "2017-05-09 08:09:22"]
	start_ts_list=["2016-05-10 12:24:22", "2016-06-09 22:12:36", "2016-07-10 01:38:09", "2017-05-09 08:09:22"]
	end_ts_list=["2016-05-10 12:24:22", "2016-06-09 22:12:36", "2016-07-10 01:38:09", "2017-05-09 08:09:22"]

	if (count%15 == 0):
		user_id = ""
	else:
		user_id = "U" + str(randint(100,120))

	song_id = "S" + str(randint(200,210))

	if (count%11 == 0):
		artist_id = ""
	else:
		artist_id = "A" + str(randint(300,305))

	timestamp = choice(timestamp_list)
	start_ts = choice(start_ts_list)
	end_ts = choice(end_ts_list)

	if (count%12 == 0):
		geo_cd = ""
	else:
		geo_cd = choice(geo_cd_list)

	station_id = "ST" + str(randint(400,415))
	song_end_type = choice(song_end_type_list)
	like = str(randint(0,1))
	dislike = str(randint(0,1))
	file.write("<record>\n")
	file.write("<user_id>%s</user_id>\n" % (user_id))
	file.write("<song_id>%s</song_id>\n" % (song_id))
	file.write("<artist_id>%s</artist_id>\n" % (artist_id))
	file.write("<timestamp>%s</timestamp>\n" % (timestamp))
	file.write("<start_ts>%s</start_ts>\n" % (start_ts))
	file.write("<end_ts>%s</end_ts>\n" % (end_ts))
	file.write("<geo_cd>%s</geo_cd>\n" % (geo_cd))
	file.write("<station_id>%s</station_id>\n" % (station_id))
	file.write("<song_end_type>%s</song_end_type>\n" % (song_end_type))
	file.write("<like>%s</like>\n" % (like))
	file.write("<dislike>%s</dislike>\n" % (dislike))
	file.write("</record>\n")

	count = count-1

file.write("</records>")
file.close()
