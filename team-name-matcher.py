import json
import tnormaliser
import pandas as pd
import jellyfish
from collections import defaultdict
import itertools
import enchant
d = enchant.Dict("en_US")

def find_team(cands, st, m=None):
        
    max_words_in_team = len(max(cands, key=lambda _: len(_.split())).split())
    words_in_string = len(st.split())
    
    if not m:
        m = set()
        
    if (not max_words_in_team) or (not words_in_string) or (max_words_in_team > words_in_string):
        return m
    
    its = itertools.tee(iter(st.split()), max_words_in_team)
    for i, _ in enumerate(range(max_words_in_team)):
        if i > 0:
            for x in range(i):
                next(its[i], None)  # i moves ahead by i - 1
    
    possible_matches = set()
                                                            
    for p in zip(*its):
        possible_matches.add(' '.join(p))
    
    cands_to_remove = set()
    pms_to_remove = set()
    
    for lev in range(3):
        
        for team in cands:
            if not lev:
                if team in possible_matches:
                    m.add(team)
                    
                    if len(m) > 1:
                        return m
                    
                    cands_to_remove.add(team)
                    pms_to_remove.add(team)
                    
                    st = ' '.join(st.replace(team, ' ').split())
            else:
                for pm in possible_matches:
                    if jellyfish.levenshtein_distance(team,pm) == lev:
                        m.add(team)
                        
                        if len(m) > 1:
                            return m
                    
                        cands_to_remove.add(team)
                        pms_to_remove.add(pm)
   
        cands = cands - cands_to_remove
        possible_matches = possible_matches - pms_to_remove

    if max_words_in_team > 1: 
                                                            
        new_cands = set()
                                                            
        for c in cands:
            if len(c.split()) == max_words_in_team:
                if max_words_in_team == 2:
                    for v in c.split():
                        if not d.check(v):
                            new_cands.add(v)
                else:
                    for cm in itertools.combinations(c.split(), max_words_in_team - 1):
                        new_cands.add(' '.join(cm))
                    if max_words_in_team > 2:
                        new_cands.add(''.join([x[0] for x in c.split()]))
        
        cands = {c for c in cands if not len(c.split()) == max_words_in_team} | new_cands
        find_team(cands, st, m)
    
    return m if m else None
        