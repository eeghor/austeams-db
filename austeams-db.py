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
								"nickname(s)":  lambda _: [nick.split("[")[0] for nick in _.split(",")],  # nicknames are comma separated
								"founded": lambda _: str(arrow.get(_.split(";")[0], "YYYY").year),
								"ground": lambda _: [g.split('(')[0].strip() for g in _.split('\n')],   	# if multiple, they come in separate lines
								"ground capacity": lambda _: ''.join([c for c in _ if c.isdigit() or c.isspace()]).split()
	}, 
	'sponsor': lambda _: _.split('[')[0].split('(')[0].strip()}
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

		# containers for collected data
		self.team_data = []

		print('ok')

	def _scrape_team_infobox(self, team_soup):

		this_team_info = defaultdict()

		try:
			ib = team_soup.find("table", class_="infobox")
		except:
			raise Exception('can\'t find team infobox on this page!')

		for row in ib.find_all("tr"):
			
			# try to find both 'th' and 'td' that should be in the same row
			th = row.find('th')
			td = row.find('td')

			if th and td:  # proceed if both non-empty
				
				# this th will be our dictionary key
				k = th.text.encode('ascii','replace').decode().replace('?',' ').lower()
				
				# we are interested in those ths where there are not numbers in the name
				if not (set(k) & set(string.digits)):

					# the website field is a special case
					if k != 'website':
						this_team_info[k] = td.text.lower()
					else:
						this_team_info[k] = row.find('a')["href"]  # grab url not text
	
					if k in self.processors['team']:  # postprocess collected info if needed
						this_team_info[k] = self.processors['team'][k](this_team_info[k])

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
		
		# sponsors are not always sitting in the same section so need to try a few scenarios
		sp_span = team_soup.find('span', text=re.compile("Sponsorship"), attrs = {'id': 'Sponsorship'})
	
		if not sp_span:
			sp_span = soup.find('span', text=re.compile("Sponsors"), attrs = {'id': 'Sponsors'})
	
		if not sp_span:
			sp_span = soup.find('span', text="Colours and badge", attrs={'class': 'mw-headline'}) 
		
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
				afc = []
				
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
							elif len(afc) < i:
								afc.extend([self.processors['sponsor'](td.text.lower())]*rpl)
	
				# verify that all sponsor types are the same length
				if not len(kit) == len(shirt) == len(afc):
					print("some problem with sponsors!")

				break


		this_team_sponsors['previous'] = {'kit': process_sponsors(kit), 
											'shirt': process_sponsors(shirt), 
												'afc': process_sponsors(afc)}
		this_team_sponsors['current'] = {'kit': process_sponsors(kit, sponsor_now=True), 
											'shirt': process_sponsors(shirt, sponsor_now=True), 
												'afc': process_sponsors(afc, sponsor_now=True)}

		return this_team_sponsors



if __name__ == '__main__':

	sc = SportDBCreator()
	
	# start visiting urls
	for team in sc.team_urls[sc.sport]:

		r = requests.get(sc.team_urls[sc.sport][team])
	
		soup = BeautifulSoup(r.text, 'html.parser')
	
		sc.team_data.append(sc._scrape_team_infobox(soup))

	print(sc.team_data)

	print('sponsors...')

	for team in sc.team_urls[sc.sport]:

		r = requests.get(sc.team_urls[sc.sport][team])
	
		soup = BeautifulSoup(r.text, 'html.parser')

		print("team=", team)
	
		print(sc._scrape_team_sponsors(soup))


	
	# 	# search by css class
	# 	ib = soup.find("table", class_="infobox")
	
	# 	# go by rows
	# 	for row in ib.find_all("tr"):
	
	# 		th = row.find('th')
	# 		if th:
				
	# 			k = th.text.encode('ascii','replace').decode().replace('?',' ').lower()
	
	# 			if not set(k) & set(string.digits):
	# 				# any urls
	# 				if k != 'website':
	# 					team_info[k] = row.find('td').text.lower()
	# 				else:
	# 					team_info[k] = row.find('a')["href"]
	# 					# try to get sicial media details from the landing oage of the web site
	# 					offpage = requests.get(team_info[k])
	
	# 					offsoup = BeautifulSoup(offpage.text, 'html.parser')
	
	# 					#print(offpage.text)
	
	# 					socials = defaultdict()
	
	# 					try:
	
	# 						print('looking for facebook...')
	# 						fa = offsoup.find('div', class_='social-links')
							
	# 						for a1 in fa.find_all('a'):
	# 							print(a1['href'])
	# 					except:
	# 						pass
	
	# 					try:
	# 						socials['twitter'] = offsoup.find('a', class_='twitter')['href']
	# 					except:
	# 						pass
	
	# 					print(socials)
	
	
	# 				if k in processors['team']:
	# 					team_info[k] = processors['team'][k](team_info[k])
	# 		else:
	# 			pass
	
	# 	all_tmz.append(team_info)
	
	# 	# search for the first teams squad
	# 	fst_team_span = soup.find('span', id='First_team_squad')
	# 	print('fst_team_span=', fst_team_span)
	
	# 	team_countries = []
	
	# 	if fst_team_span:
	
	# 		fst_team_h3 = fst_team_span.parent
	
	# 		for s in fst_team_h3.next_siblings:
		
	# 			if s.name == 'table':
		
	# 				# find the very first td
	# 				tr = s.find('tr')
	
	# 				for flag in tr.find_all('span', class_="flagicon"):
	# 					team_countries.append(flag.find('a')["title"].lower())
		
	# 				print(Counter(team_countries))
	
	# 				altc += Counter(team_countries)
	
	# 				break	
		
	# 	# look for spornsors
	
	# 	print('looking for SPONSORS..')
		
	# 	sp_span = soup.find('span', text=re.compile("Sponsorship"), attrs = {'id': 'Sponsorship'})
	
	# 	if not sp_span:
	# 		sp_span = soup.find('span', text=re.compile("Sponsors"), attrs = {'id': 'Sponsors'})
	
	# 	if not sp_span:
	# 		sp_span = soup.find('span', text="Colours and badge", attrs={'class': 'mw-headline'}) 
	
	# 	colors_h2 = sp_span.parent
	# 	#print(colors_h2)
	
	# 	for sib in colors_h2.next_siblings:
	
	# 		if sib.name == 'h2':
	# 			print("NO SPONSOR INFO!!!")
	# 			break
	
	# 		if sib.name == 'table':
	
	# 			print("FOUND TABLE!")
	
	# 			kit = []
	# 			shirt = []
	# 			afc = []
				
	# 			for i, r5 in enumerate(sib.find_all('tr')):
					
	# 				if i == 0:
	# 					continue
	
	# 				print([_.text for _ in r5.find_all('td')])
	
	
	# 			break
	
	# 	# get IMAGE
	# 	print('BACKGROUND COLORS:')
	
	# 	team_cols = set()
	
	# 	imgs = soup.find('td', attrs={'class': 'toccolours'})
		
	# 	if imgs:
	# 		for ss in imgs.find_all('div', style=re.compile('background-color')):
	# 			colcode = ss["style"].split('background-color:')[-1].replace(';','').strip()
	# 			# print(colcode)
	# 			if len(colcode) == 7:
	# 				try:
	# 					c = webcolors.hex_to_name(webcolors.normalize_hex(colcode))
	# 					# print("color name: ", c)
	# 				except:
	# 					# print('don\'t know color name')
	
	# 					rgb_triplet = webcolors.hex_to_rgb(colcode)
	
	# 					min_colours = {}
	
	# 					for key, name in webcolors.CSS3_HEX_TO_NAMES.items():
	
	# 						r_c, g_c, b_c = webcolors.hex_to_rgb(key)
	# 						rd = (r_c - rgb_triplet[0]) ** 2
	# 						gd = (g_c - rgb_triplet[1]) ** 2
	# 						bd = (b_c - rgb_triplet[2]) ** 2
	# 						min_colours[(rd + gd + bd)] = name
	
	# 					c =  min_colours[min(min_colours.keys())]
	
	# 				team_cols.add(c)
	
	# 	print("team colors: ", team_cols)
	
	# 	# LOGO
	
	# 	print("LOGO")
	
	# 	im_logo = soup.find('a', class_='image')
	
	# 	so = im_logo.find('img')['src']
	
	# 	downl_im = requests.get('https:' + so)
	# 	with open('logofile.png', 'wb') as f:
	# 		f.write(downl_im.content)
	
	# 	i1 = cv2.imread('logofile.png')
	# 	print('got image {}'.format(i1.shape))
	
	# 	rgbs = []
	
	# 	for x in range(i1.shape[0]):
	# 		for y in range(i1.shape[1]):
	# 			bgr = list(i1[x,y,:])
	# 			rgbs.append(tuple(bgr[::-1]))
	
	# 	print(Counter(rgbs).most_common(5))
	
	# 	#print({webcolors.rgb_to_name(r[0]): r[1] for r in Counter(rgbs).most_common(5)})
	
	
	# 	# VENUE
	
	# 	th1 = soup.find('table', class_='infobox').find('th', text='Ground')
	
	# 	for i, s in enumerate(th1.next_siblings, 1):
	# 		if s.name == 'td':
	# 			venue_url = "https://en.wikipedia.org" + s.find('a')['href']
	# 			break
	
	# 	venue_resp = requests.get(venue_url)
	
	# 	venue_soup = BeautifulSoup(venue_resp.text, 'html.parser')
	
	# 	venue_infobox = venue_soup.find('table', class_='infobox')
	
	# 	venue_data = dict()
	
	# 	for row in venue_infobox.find_all('tr'):
	# 		th = row.find('th')
	# 		if th:
	# 			local_td = row.find('td')
	# 			if th and local_td:
	
	# 				print("th=", th)
	# 				print("td=", row.find('td'))
	# 				venue_data.update({th.text.lower(): local_td.text})
	
	# 	print(venue_data)
	
	
	
	# 	break
	
	
	# 	print(altc.most_common())
		
	# 	json.dump(all_tmz, open('teamz.json', 'w'))
	
	
	# 