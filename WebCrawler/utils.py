def dms_to_decimal(dms_str):
    # Split the string into degree, minute, second, and direction
    parts = dms_str[:-1].replace('°', ' ').replace("'", ' ').replace('"', '').split()
    degrees = float(parts[0])
    minutes = float(parts[1])
    seconds = float(parts[2])
    direction = dms_str[-1]
    
    # Convert to decimal degrees
    decimal = degrees + minutes / 60 + seconds / 3600
    
    # If the direction is South or West, make the value negative
    if direction in ['S', 'W']:
        decimal = -decimal
    
    return decimal

def scroll_shim(passed_in_driver, object):
    x = object.location['x']
    y = object.location['y']
    scroll_by_coord = 'window.scrollTo(%s,%s);' % (
        x,
        y
    )
    scroll_nav_out_of_way = 'window.scrollBy(0, -120);'
    passed_in_driver.execute_script(scroll_by_coord)
    passed_in_driver.execute_script(scroll_nav_out_of_way)