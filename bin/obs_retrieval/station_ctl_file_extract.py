# Abstract:
#
#               This script is used to extract information from the station control file
#               The input here is the path to the stationctl file, it should be input like:
#               This lines are used to 1) open the ctl file, 2) split it in lines, 3) grab the lines that have station name, lat and long (every other line),
#               4) split these line based on spaces, 5) remove the spaces, 6) create 2 new lists, one with name info (ID, source, name) and one with lat lon datum etc.
#


### Functions:
def station_ctl_file_extract(ctlfile_Path):
    '''
    The input here is the path to the stationctl file, it should be input like:
    
    '''
    
    ctlfile = open(r'{}'.format(ctlfile_Path)).read() 
    lines = ctlfile.split('\n')
    lines1 = lines[0::2]
    lines1 = [i.split('"') for i in lines1]
    lines1 = [list(filter(None, i)) for i in lines1]
    lines1_format=[]
    try:
        for i in lines1:
            first = i[0].split(' ')[0]
            second = i[0].split(' ')[1]
            source = second.split('_')[-1]
            third = i[1]
            lines1_format.append([first,second,third,source])
    except:
        pass
            
    lines1 = lines1_format

    lines2 = lines[1::2]
    lines2 = [i.split(' ') for i in lines2]
    lines2 = [list(filter(None, i)) for i in lines2]
    
    return lines1,lines2
