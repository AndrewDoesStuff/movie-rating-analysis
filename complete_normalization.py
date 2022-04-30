import pandas as pd
from paths import t_path, m_m_path, m_r_path, debug as p



def tomatoes_normalization(outputName="normalized_tomatoes.csv"):
    tomatoes = pd.read_csv(t_path)
    normalized_data = ["tomatometer_rating", "genres"]
    tomatoes = tomatoes.drop(tomatoes.columns.difference(normalized_data), axis=1)
    tomatoes = tomatoes.dropna()
    tomatoes['genres'] = tomatoes['genres'].str.split(",").str[0]
    normalized_rating = list(tomatoes['tomatometer_rating'])
    normalized_rating = [int(i) for i in normalized_rating]
    normalized_rating = ratingsMapper(normalized_rating)
    tomatoes = tomatoes.drop('tomatometer_rating',axis=1)
    tomatoes['ratings'] = normalized_rating
    tomatoes.to_csv(outputName, header=False, index=False)


def movielens_normalization(outputName="normalized_movielens.csv"):
    movies = pd.read_csv(p + m_m_path)
    ratings = pd.read_csv(p + m_r_path)
    movies['genres'] = movies['genres'].str.split("|").str[0]
    movies = movies.drop('title', axis=1)
    ratings = ratings.drop(['timestamp', 'userId'], axis=1)
    count = 0 
    genre_list = []
    rating_list = []
    count = 0
    for i in ratings["movieId"]:
        genre = movies[movies['movieId'] == i] # genre! 
        rating = ratings['rating'][count] # rating!
        genre = list(genre['genres'])[0]
        genre_list.append(genre)
        rating_list.append(rating)
        count += 1
        if count > 1000000:
            break
    normalized_dataframe = pd.DataFrame(list(zip(genre_list,rating_list)), columns=["genre", "rating"])
    normalized_dataframe.to_csv(outputName,header=False, index=False)
      
    

def ratingsMapper(ratings):
    newSol = []
    for i in ratings:
        i = round(i / 20, 1)
        newSol.append(i)
    return newSol

# tomatoes_normalization()
movielens_normalization()