from bs4 import BeautifulSoup
import requests
import json
import sys
from collections import defaultdict, Counter
import arrow
import string
import webcolors
import re
import cv2
import numpy as np
import os
import shutil
from itertools import chain
from tnormaliser import StringNormalizer

from abc import ABCMeta, abstractmethod

tn = StringNormalizer(remove_stopwords=True, remove_punctuation=True, 
						lowercase=True, short_state_names=True, 
							full_city_names=True, remove_nonalnum=True, disamb_country_names=True,
								ints_to_words=False, year_to_label=False, remove_dupl_subsrings=True, max_dupl=4,
									remove_dupl_words=False)

class TEGCodeFinder:
	
	def __init__(self):
		
		self.AUS_SUBURBS = json.load(open('/Users/ik/Data/suburbs-and-postcodes/aus_suburbs_auspost_APR2017.json', 'r'))
		self.TEG_VENUES = json.load(open('../temp_venue_match/teg_venues_anz.json'))
		self.STATES_AND_REGIONS = {s['state'] for l in self.AUS_SUBURBS for s in self.AUS_SUBURBS[l]} | {v['state'] for v in self.TEG_VENUES}
		
	def _find_state_by_suburb(self, st_norm):
		'''
		find suburb and then the corresp. state in NORMALISED string st
		'''   
		suburb_candidates = set()
		
		for w in st_norm.split():
			if w[0] in self.AUS_SUBURBS:
				for s in self.AUS_SUBURBS[w[0]]:         
					_ = re.search(r'\b' + s['name'] + r'\b', st_norm)
					if _:
						suburb_candidates.add((s['name'], s['state']))
				if len(suburb_candidates) == 1:
					return list(suburb_candidates).pop()[1]
				elif len(suburb_candidates) > 1:
					# pick the state corresp. to the longest suburb name
					return max(suburb_candidates, key=lambda _: len(_[0].split()))[1]
				else:    # no suburbs found in location
					return None
		
	def _get_venue_state(self, venue_record):
		
		if 'location' in venue_record:
			
			loc_norm = tn.normalise(venue_record['location'])
	 
			state = list(set(loc_norm.split()) & self.STATES_AND_REGIONS)
			if state:  
				return state.pop()
		
		# no state in location; search the url
		if 'wiki_url' in venue_record:
			url_state = list(set(tn.normalise(venue_record['wiki_url']).split()) & self.STATES_AND_REGIONS)
			if url_state:
				return url_state.pop()
		
		if 'location' in venue_record:
			state_loc_sub = self._find_state_by_suburb(loc_norm)
			if state_loc_sub:
				return state_loc_sub
		
		if 'known_as' in venue_record:
			for name in venue_record['known_as']:
				state_known_as = self._find_state_by_suburb(tn.normalise(name))
				if state_known_as:
					return state_known_as         
		
	def find_teg_code(self, venue_record):
		
		'''
		returns venue state and venue TEG code(s) for a venue_record
		'''
		found_tegcodes = []  # teg codes for this venue

		venue_state = self._get_venue_state(venue_record)
		
		if venue_state:
	
			for teg_venue in self.TEG_VENUES:
			
				if teg_venue['name'].strip():
					if re.search(r'\b' + tn.normalise(venue_record['name'])  + r'\b', tn.normalise(teg_venue['name'])) and (venue_state.lower() == teg_venue['state'].lower()):
						found_tegcodes.append(teg_venue['teg_code'])
			
			if ('known_as' in venue_record):
				for teg_venue in self.TEG_VENUES:
					for former_name in venue_record['known_as']:
						if teg_venue['name'].strip():
							if re.search(r'\b' + tn.normalise(former_name) + r'\b', tn.normalise(teg_venue['name'])) and (venue_state.lower() == teg_venue['state'].lower()):
								found_tegcodes.append(teg_venue['teg_code'])

		return {'state': venue_state, 'teg_code': list(set(found_tegcodes))}


class BaseSportDBCreator(metaclass=ABCMeta):

	@abstractmethod
	def _is_sport_supported(self):
		'''
		check that we can create database for this sport
		'''
		pass

class SportDBCreator(BaseSportDBCreator):

	def _is_sport_supported(self):

		if self.sport not in self.team_urls:
			return False
		else:
			return True

	def _setup_processors(self):

		self.processors = {'team': 
							{"full name": lambda _: _.split("[")[0],
							"nickname(s)":  lambda _: [nick.split("[")[0].strip() for nick in _.split(",")],  # nicknames are comma separated
							"founded": lambda _: str(arrow.get(_.split(";")[0], "YYYY").year),
							"ground": lambda _: [g.split('(')[0].strip() for g in _.split('\n')],   	# if multiple, they come in separate lines
							"ground capacity": lambda _: ''.join([c for c in _ if c.isdigit() or c.isspace()]).split(),
							'history': lambda _: [w.strip() for w in _.split('\n') if len([p for p in w if p.isdigit()]) < 4],
							'arena': lambda _: [w.strip() for w in _.split('\n') if len([p for p in w if p.isdigit()]) < 4],
							'arena capacity': lambda _: [w for w in _.replace(',','').split() if w.isdigit()],
							'location': lambda _: _.replace("\n", ' '),
							'team colors': lambda _: [w.strip() for w in _.split(',')]}, 
							'sponsor': lambda _: _.split('[')[0].split('(')[0].strip(),	
							"venue": {"former names": lambda _: [g.split('(')[0].strip() for g in _.split('\n')],
										"owner": lambda _: [g.split('(')[0].strip() for g in _.split('\n')],
										"operator": lambda _: [g.split('(')[0].strip() for g in _.split('\n')],
										# "capacity": lambda _: _.split("[")[0],
										"field size": lambda _: 'x'.join([c for c in _.split() if c.isdigit()]),
										"opened": lambda _: str(arrow.get(_.split(";")[0], "YYYY").year),
										"location": lambda _: ','.join([w.strip() for w in _.split('\n')]),
										"surface": lambda _: _,
										"expanded": lambda _: _,
										"renovated": lambda _: _,}}


		return self


	def __init__(self):

		print('initializing class...', end='')

		# do we have DATA directory? if we do, what about the team url files? if no directory exists, throw error
		if not os.path.isdir('data'):
			raise Exception('data directory is absent!')

		# try to read the team wiki urls file
		try:
			self.team_urls = json.load(open('data/team-wiki-urls.json','r'))
		except:
			raise Exception('you need to have a file with team wikipedia urls in data directory!')

		self.sport = sys.argv[1]

		if not self._is_sport_supported():
			raise Exception(f'sport {self.sport} is not currently supported, come back later..')

		self._setup_processors()

		self.socials_of_interest = 'facebook instagram youtube twitter'.split()

		# prepopulate containers for collected data
		self.team_data = [{"name": team, "sport": self.sport, "wiki_url": self.team_urls[self.sport][team]} for team in self.team_urls[self.sport]]
		self.venue_data = []

		print('ok')

		self.GROUND_SYNS = {'ground', 'complex', 'oval', 'hub', 'park', 'showgrounds', 'stadium', 
								'field', 'reserve', 'estadio', 'club', 'sport', 'arena', 'sports', 'the', 'centre', 'center'}
		self.LEAGUE_NMS = {'npl': 'national premier leagues', 'afl': 'australian football league',
								'nbl': 'national basketball league'}

		self.RE_YEAR = re.compile(r'\d{4}')
		self.RE_COLOR = re.compile('(?<=\")[a-zA-Z ]+(?=\")')

	def _scrape_team_infobox(self, team_soup):

		"""
		team_soup is a soup object for the team
		returns team information from the team's wikipedia infobox as a dictionary
		"""

		this_team_info = defaultdict()

		try:
			ib = team_soup.find("table", class_="infobox")
		except:
			raise Exception('can\'t find team infobox on this page!')

		_ths = []

		for row in ib.find_all("tr"):
			
			# try to find both 'th' and 'td' that should be in the same row
			th = row.find('th')
			td = row.find('td')

			if th:  

				heading = th.text.lower()

				if not th.text.strip():
					heading = _ths.pop()
				else:
					_ths.append(heading)

				if 'union' in heading:
					k = 'union'
				elif 'nick' in heading:
					k = 'nickname'
				elif 'locat' in heading:
					k = 'location'
				elif (('ground' in heading) or ('arena' in heading)) and ('capacity' not in heading):
					k = 'ground'
				elif (('league' in heading) or ('competition' in heading)) and (len(heading.split()) < 3):
					k = 'league'
				elif 'website' in heading:
					k = 'website'
				elif 'history' in heading:
					k = 'known_as'
				elif 'colo' in heading:
					k = 'colours'
				else:

					continue
				
				# this th will be our dictionary key
				# k = th.text.encode('ascii','replace').decode().replace('?',' ').lower().strip()

				if td:  # 2-column scenario

					if k in ['union', 'location']:
						this_team_info[k] = td.text.replace("\n",' ').lower().strip()
					elif k == 'league':
						for l in list({lg.lower().strip().split('(')[0] for lg in td.text.split("\n")}):
							for rpl in self.LEAGUE_NMS:
								l = l.replace(rpl, self.LEAGUE_NMS[rpl])
							if k in this_team_info:
								this_team_info[k].append(l)
							else:
								this_team_info[k] = [l]

					elif k == 'nickname':
						this_team_info[k] = [nick for nick in list({lg.lower().split('[')[0].split('(')[0].strip() 
												for lg in re.split(r'[\n;,]\s*', td.text)}) if len(nick) > 1]
					elif k == 'colours':
						this_team_info[k] = list(set([l.lower().strip() for l in re.split(r'[\n;,  ]\s*', td.text) if 
													(':' not in l) and l.strip() not in {'and', '&', ''}]))
					elif k == 'founded':
						if self.RE_YEAR.search(td.text):  # avoid empty search results
							this_team_info[k] = min(self.RE_YEAR.findall(td.text), key=lambda _: int(_))
						else:
							this_team_info[k] = None
					elif k == 'website':
						this_team_info[k] = row.find('a')["href"]  # grab url not text
					elif k == 'known_as':
						this_team_info[k] = [t[0] for t in zip([_.text.strip().lower() for _ in td.find_all('b')], 
												[line.lower() for line in td.text.split('\n') if re.search(r'\d{4}', line)])
													if 'present' not in t[1]]

					elif k == 'ground':

						as_ = td.find_all('a', attrs={'title': True})

						_ = []

						if len(as_) == 1:
							_.append({'name': tn.normalise(as_[0].text.lower().strip()), 'wiki_url': 'https://en.wikipedia.org' + as_[0]['href']})
						else:
							for g in as_:
								if ((set(g.text.replace(',',' ').lower().split()) & self.GROUND_SYNS) or 
										(set(g['title'].replace(',',' ').replace('_',' ').lower().split()) & self.GROUND_SYNS)):
									_.append({'name': tn.normalise(g.text.lower().strip()), 'wiki_url': 'https://en.wikipedia.org' + g['href']})

						if k in this_team_info:
							venue_names_already_there = {v['name'] for v in this_team_info[k]}
							[this_team_info[k].append(v) for v in _ if v["name"] not in venue_names_already_there]
						else:
							this_team_info[k] = _
					
				elif th.has_attr('colspan') and (k == 'website'):
					this_team_info[k] = row.next_sibling.next_sibling.find('a')['href']
					continue

		return this_team_info

	def _scrape_team_sponsors(self, team_soup):

		
		def process_sponsors(sponsor_list, sponsor_now=False):

			assert isinstance(sponsor_now, bool), 'incorrestly chosen option - only True or False allowed!'

			if not sponsor_list:
				return None

			if not sponsor_now:
				sponsors = {s.lower() for s in sponsor_list[:-1] if s != sponsor_list[-1]}
				if not sponsors:
					return None
			else:
				sponsors = [sponsor_list[-1]]

			# sponsors may be separates by new line or / or ,
			sponsors = {z.strip() for s in sponsors 
						for v in s.split('\n') 
							for r in v.split('/') 
								for z in r.split(',') if z.strip() and z.strip().isalnum()}	

			return list(sponsors) if sponsors else None	


		this_team_sponsors = defaultdict()
		this_team_sponsors['sponsors'] = defaultdict()
		
		# sponsors are not always sitting in the same section so need to try a few scenarios
		sp_span = team_soup.find('span', text=re.compile("Sponsorship"), attrs = {'id': 'Sponsorship'})
	
		if not sp_span:
			sp_span = team_soup.find('span', text=re.compile("Sponsors"), attrs = {'id': 'Sponsors'})
	
		if not sp_span:
			sp_span = team_soup.find('span', text="Colours and badge", attrs={'class': 'mw-headline'}) 
		
		# maybe there's no sponsor info, then just return an empty dict
		if not sp_span:
			return this_team_sponsors

		for sib in sp_span.parent.next_siblings:
	
			if sib.name == 'h2':
				# apparently, no sponsor info, return empty dict
				return this_team_sponsors
	
			if sib.name == 'table':

				kit = []
				shirt = []
				other = []
				
				for i, row in enumerate(sib.find_all('tr')):
					
					if i == 0:    # skip header
						continue

					if (i == 1) and row.find('th'):
						continue

					any_tds = row.find_all('td')

					if any_tds:

						for j, td in enumerate(any_tds):

							if j == 0:
								continue

							try:
								rpl = int(td['rowspan'])
							except:
								rpl = 1
							
							# try to add to sponsor types, left to right
							if len(kit) < i:
								kit.extend([self.processors['sponsor'](td.text.lower())]*rpl)
							elif len(shirt) < i:
								shirt.extend([self.processors['sponsor'](td.text.lower())]*rpl)
							elif len(other) < i:
								other.extend([self.processors['sponsor'](td.text.lower())]*rpl)
	
				# verify that all sponsor types are the same length
				if not len(kit) == len(shirt) == len(other):
					print("some problem with sponsors!")

				break


		this_team_sponsors['sponsors']['previous'] = {'kit': process_sponsors(kit), 
											'shirt': process_sponsors(shirt), 
												'other': process_sponsors(other)}
		this_team_sponsors['sponsors']['current'] = {'kit': process_sponsors(kit, sponsor_now=True), 
											'shirt': process_sponsors(shirt, sponsor_now=True), 
												'other': process_sponsors(other, sponsor_now=True)}

		return this_team_sponsors

	def _scrape_socials(self, team_website_url):

		team_socials = defaultdict()

		soup = BeautifulSoup(requests.get(team_website_url).text, "lxml")

		for a in soup.find('div', class_='social-links').find_all('a'):
			for soc in self.socials_of_interest:
				if soc in a['href']:
					team_socials[soc] = a['href']


		return {'social_media_accounts': team_socials}

	def _scrape_squad(self, team_soup):

		fst_team_span = team_soup.find('span', id='First_team_squad')
	
		team_countries = []
	
		if not fst_team_span:
			return team_countries
		
		for s in fst_team_span.parent.next_siblings:
		
			if s.name == 'table':
		
				# find the very first td
				tr = s.find('tr')
	
				for flag in tr.find_all('span', class_="flagicon"):
					team_countries.append(flag.find('a')["title"].lower())
		
				return {"player_citizenships": Counter(team_countries)}

		return team_countries

	def _scrape_team_colors(self, team_soup):

		def find_nearest_color(hex_color):

			# make hex rgb
			rgb_triplet = webcolors.hex_to_rgb(hex_color)
	
			min_colours = defaultdict() # {score: color name,..}

			for key, name in webcolors.CSS3_HEX_TO_NAMES.items():

				r_c, g_c, b_c = webcolors.hex_to_rgb(key)

				rd = (r_c - rgb_triplet[0]) ** 2
				gd = (g_c - rgb_triplet[1]) ** 2
				bd = (b_c - rgb_triplet[2]) ** 2

				min_colours[rd + gd + bd] = name

			return(min_colours[min(min_colours.keys())])

		team_colors = defaultdict(lambda: defaultdict(list))

		# background colors first (kit)
		imgs = team_soup.find('td', attrs={'class': 'toccolours'})
		
		if imgs:

			hexs = []

			for ss in imgs.find_all('div', style=re.compile('background-color')):

				colcode = ss["style"].split('background-color:')[-1].replace(';','').strip()

				if len(colcode) == 7:
					hexs.append(colcode)

			for t in Counter(hexs).most_common(5):

				try:
					c = webcolors.hex_to_name(webcolors.normalize_hex(t[0]))
				except:
					c = find_nearest_color(t[0])

				team_colors['kit']['hex'].append(t[0])
				team_colors['kit']['name'].append(c)

			team_colors['kit']['name'] = list(set(team_colors['kit']['name']))

		# team logos

		if not os.path.isdir('data_temp'):
			os.mkdir('data_temp')

		im_logo = team_soup.find('a', class_='image')
	
		with open('data_temp/logofile.png', 'wb') as f:
			f.write(requests.get('https:' + im_logo.find('img')['src']).content)
	
		i1 = cv2.imread('data_temp/logofile.png')
	
		rgbs = []
	
		for x in range(i1.shape[0]):
			for y in range(i1.shape[1]):

				bgr = list(i1[x,y,:])
				rgbs.append(tuple(bgr[::-1]))

		for t in Counter(rgbs).most_common(5):

			try:
				c = webcolors.rgb_to_name(t[0])
			except:
				c = find_nearest_color(webcolors.rgb_to_hex(t[0]))

			team_colors['logo']['hex'].append(webcolors.rgb_to_hex(t[0]))
			team_colors['logo']['name'].append(c)
		
		shutil.rmtree('data_temp')

		team_colors['logo']['name'] = list(set(team_colors['logo']['name']))

		return {"team_colors": team_colors}

	def _scrape_venues(self, venue_soup):

		venue_data = defaultdict()
		
		venue_infobox = venue_soup.find('table', class_='infobox')	

		if not venue_infobox:
			return venue_data	

		for row in venue_infobox.find_all("tr"):
			
			# try to find both 'th' and 'td' that should be in the same row
			th = row.find('th')
			td = row.find('td')

			if th:  

				heading = th.text.lower()

				if ('establ' in heading) or ('opened' in heading) or ('founded' in heading):
					k = 'established'
				elif 'capacity' in heading:
					k = 'capacity'
				elif 'locat' in heading:
					k = 'location'
				elif 'coord' in heading:
					k = 'coordinates'
				elif 'own' in heading:
					k = 'owner'
				elif ('former' in heading) and ('name' in heading):
					k = 'known_as'
				else:
					continue	

				if td:  # 2-column scenario
	
					if k in ['location']:
						venue_data[k] = td.text.replace("\n",' ').lower().strip().split('(')[0]
					elif k == 'established':
						venue_data[k] = self.RE_YEAR.search(td.text).group(0)
					elif k == 'capacity':
						venue_data[k] = re.search(r'\d+,*\d+', td.text).group(0).replace(',','')
					elif k == 'coordinates':
						_ = td.find('span', class_='geo-dec').parent.find('span', class_='geo').text.split(';')
						venue_data[k] = {'lat': _[0].strip(), 'lng': _[1].strip()}
					elif k == 'owner':
						venue_data[k] = [v.lower().split('(')[0].strip() for v in re.split(r'[\n;,]\s*', td.text) if v.strip()]
					elif k == 'known_as':
						venue_data[k] = [v.lower().split('(')[0].strip() for v in re.split(r'[\n;,]\s*', td.text) if v.strip()]
						# also try to find more former or alternative names in the first paragraph of main text where these would be in bold
					else:
						pass

		for s in venue_infobox.next_siblings:
			if s.name == 'p':
				for b in s.find_all('b'):
					text_in_bold = b.text.lower().strip()
					if 'known_as' not in venue_data:
						if len(text_in_bold) > 1:
							venue_data['known_as'] = [text_in_bold]
					else:
						if ((len(text_in_bold) > 1) and 
								(text_in_bold not in venue_data['known_as'])):
							venue_data['known_as'].append(text_in_bold)
				break  # consider only the very first paragraph


		return venue_data

	def get_team_info(self):

		
		for team in self.team_urls[self.sport]:
			print(f'collecting basic team info for {team.upper()}...', end='')
			for rec in self.team_data:
				if rec['name'] == team:
					rec.update(self._scrape_team_infobox(BeautifulSoup(requests.get(self.team_urls[self.sport][team]).text, 'html.parser')))
					break
			print('ok')

		return self

	def get_team_venues(self):

		for team in self.team_urls[self.sport]:
			print(f'collecting venue info for {team.upper()}...', end='')
			for rec in self.team_data:
				if rec['name'] == team:
					if 'ground' in rec and rec['ground']:
						for r in rec['ground']:
							venue_record = {**r, **self._scrape_venues(BeautifulSoup(requests.get(r['wiki_url']).text, 'html.parser'))}
							# update with TEG codes and states
							venue_record.update(tcf.find_teg_code(venue_record))
							self.venue_data.append(venue_record)
						

			print('ok')

		return self

	def get_team_sponsors(self):

		print('collecting team sponsors...', end='')

		for team in self.team_urls[self.sport]:

			for rec in self.team_data:
				if rec['name'] == team:
					rec.update(self._scrape_team_sponsors(BeautifulSoup(requests.get(self.team_urls[self.sport][team]).text, 'html.parser')))
					break

		print('ok')

		return self

	def get_int_profile(self):

		print('collecting player citizenships...', end='')

		for team in self.team_urls[self.sport]:

			for rec in self.team_data:
				if rec['name'] == team:
					rec.update(self._scrape_squad(BeautifulSoup(requests.get(self.team_urls[self.sport][team]).text, 'html.parser')))
					break

		print('ok')

		return self

	def get_team_colors(self):

		print('collecting team colors...', end='')

		for team in self.team_urls[self.sport]:

			for rec in self.team_data:
				if rec['name'] == team:
					rec.update(self._scrape_team_colors(BeautifulSoup(requests.get(self.team_urls[self.sport][team]).text, 'html.parser')))
					break

		print('ok')

		return self


	def get_team_social_media(self):

		print('collecting team social media account info...', end='')

		for team in self.team_urls[self.sport]:

			for rec in self.team_data:
				if rec['name'] == team and rec['website']:
					rec.update(self._scrape_socials(rec['website']))
					break

		print('ok')

		return self

if __name__ == '__main__':

	tcf = TEGCodeFinder()

	sc = (SportDBCreator()
			.get_team_info().get_team_venues())
				# .get_team_sponsors()
				# 	.get_int_profile()
				# 		.get_team_colors()
				# 			.get_team_social_media())

	json.dump(sc.team_data, open('teaminfo-' + sc.sport.replace(' ','').upper() + '.json', 'w'))
	json.dump(sc.venue_data, open('venueinfo-' + sc.sport.replace(' ','').upper() + '.json', 'w'))