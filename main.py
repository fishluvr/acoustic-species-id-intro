import babypandas as bpd
import pandas as pd
import numpy as np
import random

def get_hour(datetime):
    return int(str(datetime).split('Recorded at ')[1].split(':')[0])

# Selects a random clip:
def get_random_index(stopInt):
    return random.randint(0, stopInt);


# Reads data from csv file
audiomoth_data = bpd.read_csv('data/Peru_2019_AudioMoth_Data_Full.csv') 

# Audiomoth data w/ successful recordings (1 minute clips)
# Removes problematic Audiomoths (21, 19, 8, 28)      
successful_audiomoth_data = audiomoth_data[(audiomoth_data.get('Duration') >= 60) & (audiomoth_data.get('StartDateTime') != 'NaN')
    & (audiomoth_data.get('AudioMothCode') != 'AM-21') & (audiomoth_data.get('AudioMothCode') !=  'AM-19') 
    & (audiomoth_data.get('AudioMothCode') !=  'AM-8') & (audiomoth_data.get('AudioMothCode') != 'AM-28')]

# Audiomoth data (1+ minute long & w/ start-hour) 
successful_audiomoth_data = (successful_audiomoth_data
    .assign(Hour = successful_audiomoth_data.get('Comment').apply(get_hour)))

# Number of recordings per given hour
# Hours in UTC Time
audiomoth_data_hours = successful_audiomoth_data.groupby('Hour').count()

# 31 Audiomoths total (excluding the problematic ones)
# total = len(successful_audiomoth_data.get('AudioMothCode').unique())
audiomoth_code_list = ['AM-1', 'AM-10', 'AM-11', 'AM-12', 'AM-13', 'AM-14', 'AM-15',
    'AM-16', 'AM-17', 'AM-18', 'AM-2', 'AM-20', 'AM-22', 'AM-23',
    'AM-24', 'AM-25', 'AM-26', 'AM-27', 'AM-29', 'AM-3', 'AM-30',
    'AM-4', 'AM-5', 'AM-6', 'AM-7', 'AM-9', 'WWF-1', 'WWF-2', 'WWF-3',
    'WWF-4', 'WWF-5']

stratified_sample = successful_audiomoth_data.take([0])
stratified_sample

# Loop through all 31 AudioMoths
for a in range(31):
    audiomoth_current_code = successful_audiomoth_data[successful_audiomoth_data.get('AudioMothCode') == audiomoth_code_list[a]] # change 'AM-1' to 'audiomoth_code_list[a]' later

    # 24 clips per AudioMoth (1 per hour)
    for i in range(24):
        audiomoth_data_clips = audiomoth_current_code[audiomoth_current_code.get('Hour') == i].reset_index() # change to '== i' later
        audiomoth_data_clips

        # Get a random index between 0 --> (audiomoth_data_clips.shape[0])
        random_index = get_random_index(audiomoth_data_clips.shape[0])

        # Place row from 'audiomoth_data_clips' @ index 'random_index' into a new DataFrame
        random_clip = audiomoth_data_clips[audiomoth_data_clips.index == random_index]
        stratified_sample = stratified_sample.append(random_clip, ignore_index = True)

# Return new DataFrame at end
stratified_sample = stratified_sample.take(np.arange(1, stratified_sample.shape[0]))
stratified_sample

stratified_sample_csv = stratified_sample.to_csv('data/file_name_here.csv', index=False)