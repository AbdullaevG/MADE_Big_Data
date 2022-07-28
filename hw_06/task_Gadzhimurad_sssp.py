from pyspark.sql import SparkSession
from pyspark.sql.functions import col


path = "/data/twitter/twitter.txt"
end_user = 34
start_user = 12

spark = SparkSession.builder.appName("classifier").getOrCreate()

df = (
    spark
    .read
    .format("csv")
    .options(header=False, inferSchema=True, sep="\t")
    .load(path)
)

column_names = ['user', "follower"]
df = df.toDF(*column_names)

def get_dist(start_user, end_user):
    
    dist = 0
    
    used_users = {start_user}
    temp_followers = {start_user}
    
    while len(temp_followers) > 0:
        
        # На кого подписаны temp_followers
        temp_users = (
                        df
                        .filter(col("follower")
                        .isin(temp_followers))
                        .select(col("user"))
                        .dropDuplicates()
                        .collect()
                        )
            
        temp_users = {item['user'] for item in temp_users}
        dist += 1
        if end_user in temp_users:
            break
        # Убираем тех кого уже рассматривали
        temp_users -= used_users
        
        # Добавляем в список рассмотренных
        used_users |= temp_users
        
        # temp_users -> temp_followers
        temp_followers = temp_users
        
        
    return dist


dist = get_dist(start_user, end_user)
print(dist)

