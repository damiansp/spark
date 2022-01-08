import numpy as np

DATA = '../../data'
STATES = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arkansas': 'AR',
    'Arizona': 'AZ',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'D.C.': 'DC',
    'Delaware': 'DE',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Massachusetts': 'MA',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ', 
    'New Mexico': 'NM',
    'New York': 'NY', 
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Puerto Rico': 'PR',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Texas': 'TX', 
    'Tennessee': 'TN',
    'Utah': 'UT',
    'Virginia': 'VA',
    'Vermont': 'VT',
    'Washington': 'WA',
    'Washington D.C.': 'DC',
    'Washington, D.C.': 'DC',
    'Wisconsin': 'WI',
    'West Virginia': 'WV',
    'Wyoming': 'WY'}
COLORS = ['Red', 'Orange', 'Yellow', 'Green', 'Blue', 'Brown']


def write_file(path, n_lines=100_000):
    states = np.random.choice(list(STATES.values()), size=n_lines)
    color = np.random.choice(COLORS, size=n_lines)
    num = np.random.poisson(lam=30, size=n_lines)
    with open(path, 'w') as f:
        f.write('State,Color,Count\n')
        for s, c, n in zip(states, color, num):
            f.write(f'{s},{c},{n}\n')

write_file(f'{DATA}/mm.csv')
