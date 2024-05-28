import logging

print('setting log')
logging.basicConfig() #level=logging.DEBUG)
logging.getLogger('root').setLevel(logging.DEBUG)
#logging.getLogger('map_visits_to_session').setLevel(logging.DEBUG)