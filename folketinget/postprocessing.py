import pandas as pd


def postprocess():
    data_meeting = pd.read_csv('data_meeting.csv')
    data_agenda = pd.read_csv('data_agenda.csv')
    data_speech = pd.read_csv('data_speech.csv')
    
    #Data Meeting
    data_meeting1 = data_meeting[pd.notna(data_meeting['date'])]
    data_meeting1.to_csv('data_meeting.csv', index=False)
    
    
    #Data Speech
    data_speech = data_speech[pd.notna(data_speech['speech_item_id'])]
    data_speech[['agenda_item_id', 'speech_item_id']] = data_speech[['agenda_item_id', 'speech_item_id']].astype(int)
    data_speech['time_start'] = data_speech['time_start'].str.split('T').str[1]
    data_speech['time_end'] = data_speech['time_end'].str.split('T').str[1]
    data_speech.to_csv('data_speech.csv', index=False)
    
    
    #Data Agenda
    data_agenda = data_agenda[pd.notna(data_agenda['type'])]
    data_agenda[['agenda_item_id', 'type']] = data_agenda[['agenda_item_id', 'type']].astype(int)
    data_speech['speech_item_text'] = data_speech['speech_item_text'].astype(str)
    
    #Make variables from speech_data
    data_speech_group = data_speech.groupby(['meeting_id', 'agenda_item_id'])
    data_speech_group_text = data_speech_group['speech_item_text'].apply(lambda x: ' '.join(x)).reset_index()
    data_agenda = data_agenda.merge(data_speech_group_text, on=['meeting_id', 'agenda_item_id'], how='left')
    data_speech_group_time_start = data_speech_group['time_start'].first().reset_index()
    data_speech_group_time_end = data_speech_group['time_end'].last().reset_index()
    data_agenda = data_agenda.merge(data_speech_group_time_start, on=['meeting_id', 'agenda_item_id'], how='left')
    data_agenda = data_agenda.merge(data_speech_group_time_end, on=['meeting_id', 'agenda_item_id'], how='left')
    
    
    data_agenda.to_csv('data_agenda.csv', index=False)
    

postprocess()