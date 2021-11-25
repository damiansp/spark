DATA = '../../data/audioscrobbler'


raw_user_artist_data = spark.read.text(f'{DATA}/user_artist_data.txt')
raw_user_artist_data.show(5)

raw_artist_data = spark.read.text(f'{DATA}/artist_data.txt')
raw_artist_data.show(5)

raw_artist_alias = spark.read.text(f'{DATA}/artist_alias.txt')
raw_artist_alias.show(5)




