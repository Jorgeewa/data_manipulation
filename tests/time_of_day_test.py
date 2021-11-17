from data_processing.data_processing_events.data_processing_events import TimeofDayComputation, TimeofDayPeriod, TimeofDay
from datetime import datetime




def testNextQuadrantsWork():
    starting_time = datetime(2021, 11, 1, 9, 0, 1)
    starting_quadrant = TimeofDay.MORNING
    
    finishing_time = datetime(2021, 11, 2, 13, 0, 1)
    finishing_quadrant = TimeofDay.AFTERNOON
    
    t = TimeofDayComputation(starting_time, starting_quadrant, finishing_time, finishing_quadrant)
    
    x, y = next(t)
    assert x ==  datetime(2021, 11, 1, 12, 0)
    assert y ==  datetime(2021, 11, 1, 15, 59, 59)
    assert t.get_current_quadrant() ==  TimeofDay.AFTERNOON
    
    
    x, y = next(t)
    assert x ==  datetime(2021, 11, 1, 16, 0, 0)
    assert y ==  datetime(2021, 11, 1, 19, 59, 59)
    assert t.get_current_quadrant() ==  TimeofDay.EVENING
    
    
    x, y = next(t)
    assert x ==  datetime(2021, 11, 1, 20, 0, 0)
    assert y ==  datetime(2021, 11, 2, 7, 59, 59)
    assert t.get_current_quadrant() ==  TimeofDay.NIGHT
    
    
    x, y = next(t)
    assert x ==  datetime(2021, 11, 2, 8, 0)
    assert y ==  datetime(2021, 11, 2, 11, 59, 59)
    assert t.get_current_quadrant() ==  TimeofDay.MORNING
    
    
def testNumberofLoopsAndEndResult():
    starting_time = datetime(2021, 11, 1, 9, 0, 1)
    starting_quadrant = TimeofDay.MORNING
    
    finishing_time = datetime(2021, 11, 2, 13, 0, 1)
    finishing_quadrant = TimeofDay.AFTERNOON
    t = TimeofDayComputation(starting_time, starting_quadrant, finishing_time, finishing_quadrant)
    counter = 0
    for x, y in t:
        counter +=1
    assert counter == 4
    
    assert x ==  datetime(2021, 11, 2, 8, 0)
    assert y ==  datetime(2021, 11, 2, 11, 59, 59)
    assert t.get_current_quadrant() ==  TimeofDay.MORNING
    
    

