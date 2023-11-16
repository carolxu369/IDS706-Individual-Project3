-- Databricks notebook source
-- Artists Who Released the Most Songs Each Year
SELECT
  artist_name,
  count(artist_name) AS num_songs,
  year
FROM
  prepared_song_data
WHERE
  year > 0
GROUP BY
  artist_name, year
ORDER BY
  year DESC, num_songs DESC;

-- Recommendation: Identify and collaborate with the most prolific artists. These artists are not only active but likely have a significant following. Engaging in promotional activities, featured albums, or special releases with these artists can attract their fan base and boost overall platform engagement.

-- COMMAND ----------

-- Songs for DJ List Based on Tempo and Time Signature
SELECT
  artist_name,
  title,
  tempo
FROM
  prepared_song_data
WHERE
  time_signature = 4 AND
  tempo BETWEEN 100 AND 140;

-- Recommendation: Use these findings to curate specialized playlists or channels that cater to specific musical tastes, such as a DJ's selection, workout music, or party mixes. This targeted curation can improve user satisfaction and increase the time spent on the platform.

-- COMMAND ----------

-- Average Tempo of Songs Over the Years
SELECT
  year,
  AVG(tempo) AS average_tempo
FROM
  prepared_song_data
GROUP BY
  year
ORDER BY
  year;

-- Recommendation: Analyze the evolution of music trends to predict and adapt to changing listener preferences. If there's a trend toward certain tempos or styles, consider adjusting the music discovery algorithms to highlight similar new releases, potentially increasing listener engagement and satisfaction.

-- COMMAND ----------

-- Visualization of artist releases per year
SELECT
  year,
  artist_name,
  count(*) AS num_songs
FROM
  prepared_song_data
GROUP BY
  year, artist_name
ORDER BY
  year, num_songs DESC

-- Emerging Artist Focus: Use the data to identify artists with a growing number of releases each year, indicating rising popularity or increasing industry presence. Engage these emerging artists for exclusive deals, early access to new releases, or promotional events. This approach can capitalize on their growing fan base and market presence.

-- Artist Retrospectives for Established Musicians: For artists with consistent high-volume releases over a long period, consider creating special retrospectives or collections. This can include artist-focused playlists, special edition releases, or in-depth artist profiles. These retrospectives can attract long-time fans as well as new listeners who might be interested in exploring an artist's extensive catalog.

-- COMMAND ----------

