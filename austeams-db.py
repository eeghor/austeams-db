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

from abc import ABCMeta, abstractmethod

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

		print('ok')

		self.GROUND_SYNS = {'ground', 'complex', 'oval', 'hub', 'park', 'showgrounds', 'stadium', 
								'field', 'reserve', 'estadio', 'club', 'sport', 'arena', 'sports'}

		self.RE_YEAR = re.compile(r'\b\d{4}\b')
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

		for row in ib.find_all("tr"):
			
			# try to find both 'th' and 'td' that should be in the same row
			th = row.find('th')
			td = row.find('td')

			if th:  

				heading = th.text.lower()

				if 'union' in heading:
					k = 'union'
				elif 'nick' in heading:
					k = 'nickname'
				elif 'locat' in heading:
					k = 'location'
				elif ('ground' in heading) or ('arena' in heading):
					k = 'ground'
				elif ('league' in heading) or ('competition' in heading):
					k = 'league'
				elif 'website' in heading:
					k = 'website'
				elif 'history' in heading:
					k = 'former names'
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
						this_team_info[k] = [lg.lower().strip() for lg in td.text.split("\n")]
					elif k == 'nickname':
						this_team_info[k] = [lg.lower().split('[')[0].strip() for lg in td.text.split(",") if lg.lower().split('[')[0].strip()]
					elif k == 'colours':
						this_team_info[k] = [l.lower().strip() for l in re.split(r'[\n;,]\s*', td.text) if (':' not in l) and l.strip()]
					elif k == 'founded':
						if self.RE_YEAR.search(td.text):  # avoid empty search results
							this_team_info[k] = min(self.RE_YEAR.findall(td.text), key=lambda _: int(_))
						else:
							this_team_info[k] = None
					elif k == 'website':
						this_team_info[k] = row.find('a')["href"]  # grab url not text
					elif k == 'former names':
						this_team_info[k] = [_.text.strip().lower() for _ in td.find_all('b')]
					elif k == 'ground':
						_ = []
						for g in td.find_all('a', attrs={'title': True}):
							if ((set(g.text.replace(',',' ').lower().split()) & self.GROUND_SYNS) or 
									(set(g['title'].replace(',',' ').replace('_',' ').lower().split()) & self.GROUND_SYNS)):
								_.append({'name': g.text.lower().strip(), 'wiki_url': 'https://en.wikipedia.org' + g['href']})

						this_team_info[k] = _

						# this_team_info[k] = [{'name': g.text.lower().strip(), 
						# 						'wiki_url': 'https://en.wikipedia.org' + g['href']} 
						# 							for g in td.find_all('a', attrs={'title': True}) 
						# 								if (set(g.text.replace(',',' ').lower().split()) & self.GROUND_SYNS) or 
						# 								(set(g['title'].replace(',',' ').replace('_',' ').lower().split()) & self.GROUND_SYNS)]
					
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

	def _scrape_venues(self, team_soup):

		venue_data = defaultdict()

		th1 = team_soup.find('table', class_='infobox').find('th', text='Ground')

		if not th1:
			return venue_data
		
		venue_url = None

		for s in th1.next_siblings:
			if s.name == 'td':
				venue_url = "https://en.wikipedia.org" + s.find('a')['href']
				break
		if not venue_url:
			return venue_data
	
		venue_soup = BeautifulSoup(requests.get(venue_url).text, 'html.parser')
	
		venue_infobox = venue_soup.find('table', class_='infobox')			
	
		for row in venue_infobox.find_all('tr'):
			th = row.find('th')
			if th:
				local_td = row.find('td')
				if th and local_td:
		
					# print("th=", th.text)
					# print("td=", row.find('td').text)

					k = th.text.lower().strip()

					if k in self.processors['venue']:

						if k != 'coordinates':
							venue_data[k] = local_td.text.strip().lower()
						else:
							venue_data[k] = local_td.find('span', class_='geo-dec').text.strip()
			
						venue_data[k] = self.processors['venue'][k](venue_data[k])

	def get_team_info(self):

		print('collecting basic team info...', end='')

		for team in self.team_urls[self.sport]:

			for rec in self.team_data:
				if rec['name'] == team:
					rec.update(self._scrape_team_infobox(BeautifulSoup(requests.get(self.team_urls[self.sport][team]).text, 'html.parser')))
					break
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

	sc = (SportDBCreator()
			.get_team_info())
				# .get_team_sponsors()
				# 	.get_int_profile()
				# 		.get_team_colors()
				# 			.get_team_social_media())

	json.dump(sc.team_data, open('teaminfo-' + sc.sport.replace(' ','').upper() + '.json', 'w'))