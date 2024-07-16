import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from confluent_kafka import Producer, KafkaException
from datetime import datetime

def read_config():
    config = {}
    try:
        with open("client.properties") as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    config[parameter] = value.strip()
    except FileNotFoundError:
        print("Error: Configuration file 'client.properties' not found.")
    except Exception as e:
        print(f"Error reading configuration file: {e}")
    return config

def get_artist_data(sp, artist_name):
    try:
        results = sp.search(q='artist:' + artist_name, type='artist')
        artist = results['artists']['items'][0]
        artist_data = {
            'id': artist['id'],
            'name': artist['name'],
            'followers': artist['followers']['total'],
            'genres': artist['genres'],
            'popularity': artist['popularity'],
            'timestamp': datetime.now().isoformat()
        }
        return artist_data
    except Exception as e:
        print(f"Error fetching artist data: {e}")
        return None

def get_albums_data(sp, artist_id):
    try:
        albums = sp.artist_albums(artist_id, album_type='album')
        albums_data = []
        for album in albums['items']:
            albums_data.append({
                'id': album['id'],
                'artist_id': artist_id,
                'name': album['name'],
                'release_date': album['release_date']
            })
        return albums_data
    except Exception as e:
        print(f"Error fetching albums data: {e}")
        return []

def get_tracks_data(sp, album_id):
    try:
        tracks = sp.album_tracks(album_id)
        tracks_data = []
        for track in tracks['items']:
            track_info = sp.track(track['id'])
            audio_features = sp.audio_features(track['id'])[0]
            tracks_data.append({
                'id': track['id'],
                'album_id': album_id,
                'name': track['name'],
                'popularity': track_info['popularity'],
                'danceability': audio_features['danceability'],
                'energy': audio_features['energy'],
                'key': audio_features['key'],
                'loudness': audio_features['loudness'],
                'mode': audio_features['mode'],
                'speechiness': audio_features['speechiness'],
                'acousticness': audio_features['acousticness'],
                'instrumentalness': audio_features['instrumentalness'],
                'liveness': audio_features['liveness'],
                'valence': audio_features['valence'],
                'tempo': audio_features['tempo']
            })
        return tracks_data
    except Exception as e:
        print(f"Error fetching tracks data: {e}")
        return []

def produce_to_kafka(data, config, topic):
    producer = Producer(config)

    def delivery_callback(err, msg):
        if err:
            print(f'ERROR: Message failed delivery: {err}')
        else:
            key = msg.key().decode('utf-8') if msg.key() else None
            value = msg.value().decode('utf-8') if msg.value() else None
            print(f"Produced event to topic {msg.topic()}: key = {key} value = {value}")

    try:
        producer.produce(topic, value=json.dumps(data).encode('utf-8'), callback=delivery_callback)
        producer.flush()
        print(f"Produced data to topic {topic}")
    except KafkaException as e:
        print(f"Error producing to Kafka: {e}")

def main():
    config = read_config()

    # Check if the configuration was loaded correctly
    if not config:
        print("Configuration not loaded. Exiting...")
        return

    # Set up Spotify credentials
    client_id = 'your_spotify_client_id'
    client_secret = 'your_spotify_client_secret'
    sp = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))

    artist_name = 'Avenged Sevenfold'
    artist_data = get_artist_data(sp, artist_name)

    if artist_data is None:
        print("Failed to fetch artist data. Exiting...")
        return

    artist_id = artist_data['id']
    albums_data = get_albums_data(sp, artist_id)
    tracks_data = []
    for album in albums_data:
        tracks_data.extend(get_tracks_data(sp, album['id']))

    # Produce artist data
    produce_to_kafka(artist_data, config, "artist-topic")

    # Produce albums data
    for album in albums_data:
        produce_to_kafka(album, config, "album-topic")

    # Produce tracks data
    for track in tracks_data:
        produce_to_kafka(track, config, "track-topic")

if __name__ == '__main__':
    main()
