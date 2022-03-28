import itertools
import operator
import copy

import math

import random


import re
from itertools import permutations, combinations, chain

class CachedOptimalStep():
    def __init__(self, lowest_costs, best_step):
        self.lowest_costs = lowest_costs
        self.best_step = best_step

class PushPullPlan():
    def __init__(self, plan, costs):
        self.plan = plan
        self.costs = costs


#returns all permutations of a given input, starting with permutations of length "start_length" to "end_length"
#-> equals: sum k = 2 to n-1 of n choose k -> (2^n) - n - 1 - 1 (-n, because k doesnt start at 1,
#           -1 because no n choose n and -1 because k doesnt start at 0 (empty set)
def determine_permutations_of_all_relevant_lengths(eventtypes, start_length = 2, end_length = 7):
    "[A,B,C] --> [], [A], [B], [C], [A,B], [A,C], [B,C], [A,B,C]"
    result = []
    for current_subset in chain.from_iterable(combinations(eventtypes, it) for it in range(start_length, end_length+1)):
        result.append(''.join(current_subset))
        
    return result


class Initiate():
    def __init__(self, selectivity_map, eventtype_to_sources_map, outputrate_map, eventtype_to_eventtype_single_selectivities, single_selectivity_of_eventtype_within_projection, all_combinations, highest_primitive_eventtype_to_be_processed):
        self.selectivity_map = selectivity_map

        self.eventtype_to_sources_map = copy.deepcopy(eventtype_to_sources_map)
        self.outputrate_map = copy.deepcopy(outputrate_map)

        self.eventtype_to_eventtype_single_selectivities = copy.deepcopy(eventtype_to_eventtype_single_selectivities)
        self.single_selectivity_of_eventtype_within_projection = copy.deepcopy(single_selectivity_of_eventtype_within_projection)
        
        for i in range(ord('A'),ord(highest_primitive_eventtype_to_be_processed)+1):
            self.selectivity_map[str(chr(i))] = 1
            self.eventtype_to_eventtype_single_selectivities[str(chr(i))] = 1

        self.projection_to_single_eventtype_map = {}
        self.single_eventtype_to_projection_map = {}
        self.source_sent_this_type_to_node = {}
        
        self.all_combinations = all_combinations
        self.determine_selectivity_map()

        self.number_of_nodes_producing_this_projection = 1

        
        self.optimal_pull_strategy_cache = {}
    

    ######################Greedy Algorithm###############################################

    def remove_locally_produced_eventtype_from_plan_if_multi_sink_placement_greedy(self, projection_to_process):
        if not self.is_single_sink_placement(projection_to_process):
            for primitive_operator in projection_to_process.primitive_operators:
                if primitive_operator == projection_to_process.forbidden_event_types:
                    projection_to_process.primitive_operators.remove(primitive_operator)
        
        return projection_to_process

    def initiate_mapping_from_projection_to_single_eventtype_for_greedy(self,eventtypes_to_match_projection):
        char_counter = 0
        for eventtype in eventtypes_to_match_projection:
            self.projection_to_single_eventtype_map[eventtype] = (chr(ord('A')+char_counter))
            self.single_eventtype_to_projection_map[(chr(ord('A')+char_counter))] = eventtype
            char_counter+=1

    def is_complex_eventtype(self, eventtype):
        return len(eventtype) > 1
        
    def measure_mu(self, acquired_eventtypes, type_to_acquire, node):
        best_multiple_single_selectivities = 1
        best_single_sel_key = ''
        best_single_sel_keys = []
        lowest_costs = float('inf')
        if not acquired_eventtypes:
            return self.outputrate_map[type_to_acquire] * self.determine_correct_number_of_sources(node, type_to_acquire)
        else:
            single_selectivity_key = ''
            
            multiple_single_selectivities = 1
            possible_pull_combinations = determine_permutations_of_all_relevant_lengths(acquired_eventtypes, 1, end_length = (min(len(acquired_eventtypes), 3)))
                
            old_source_sent_this_type_to_node_map = copy.deepcopy(self.source_sent_this_type_to_node)

            all_prim_events_from_projection_to_acquire = self.get_sorted_primitive_eventtypes_from_projection_string(type_to_acquire)
            
            for pull_combination in possible_pull_combinations:
                for prim_event_type in all_prim_events_from_projection_to_acquire:
                    sorted_transfered_eventtypes_key = self.get_sorted_primitive_eventtypes_from_projection_string(pull_combination)
                    next_eventtype_to_pull_key = self.get_sorted_primitive_eventtypes_from_projection_string(type_to_acquire)
                    
                    sorted_all_eventtypes_key = sorted_transfered_eventtypes_key + next_eventtype_to_pull_key
                    sorted_all_eventtypes_key = self.remove_duplicates_and_sort_key(sorted_all_eventtypes_key)
                        
                    single_selectivity_key = str(prim_event_type) + '|' + sorted_all_eventtypes_key
                    
                    multiple_single_selectivities *= self.single_selectivity_of_eventtype_within_projection[single_selectivity_key]
                    
                current_costs = self.outputrate_map[type_to_acquire] * multiple_single_selectivities * self.determine_correct_number_of_sources(node, type_to_acquire)
                
                if current_costs < lowest_costs:
                    lowest_costs = current_costs
                    best_multiple_single_selectivities = multiple_single_selectivities
                    best_single_sel_key = pull_combination
                    
                #this function tries out every possible combination, therefore reset all necessary settings for next iteration
                self.source_sent_this_type_to_node = copy.deepcopy(old_source_sent_this_type_to_node_map)

                single_selectivity_key = ''
                multiple_single_selectivities = 1
            self.source_sent_this_type_to_node = copy.deepcopy(old_source_sent_this_type_to_node_map)
            return self.outputrate_map[type_to_acquire] * best_multiple_single_selectivities * self.determine_correct_number_of_sources(node, type_to_acquire), best_single_sel_key

        
    
    def greedy_single_step_plan_generator(self, projection_to_process, node):
        plan = []
        min_costs =  float('inf')
        acquired_eventtypes = []
        used_types = []
        used_types.append([])
        cheapest_type = 'X'
        total_costs = 0
        self.number_of_nodes_producing_this_projection = len(projection_to_process.node_placement)
        old_source_sent_this_type_to_node_map = copy.deepcopy(self.source_sent_this_type_to_node)
        
        self.initiate_mapping_from_projection_to_single_eventtype_for_greedy(projection_to_process.primitive_operators)
        
        projection_to_process = self.remove_locally_produced_eventtype_from_plan_if_multi_sink_placement_greedy(projection_to_process)
        
        for eventtype_to_acquire in projection_to_process.primitive_operators:
            costs = self.measure_mu(acquired_eventtypes, eventtype_to_acquire, node)
            
            if costs < min_costs:
                min_costs = costs
                cheapest_type = eventtype_to_acquire
            elif costs == min_costs and len(eventtype_to_acquire) > len(cheapest_type):
                min_costs = costs
                cheapest_type = eventtype_to_acquire
            
        
        for event_type in self.determine_all_primitive_events_of_projection(cheapest_type):
            if event_type not in acquired_eventtypes:
                acquired_eventtypes.append(event_type)    
        
        plan.append([cheapest_type])
        
        projection_to_process.primitive_operators.remove(cheapest_type)
        total_costs += min_costs
        best_single_sel = ''
        while len(projection_to_process.primitive_operators) > 0:
            min_costs =  float('inf')
            for eventtype_to_acquire in projection_to_process.primitive_operators:
                costs, used_single_sel = self.measure_mu(acquired_eventtypes, eventtype_to_acquire, node)
                
                if costs < min_costs:
                    min_costs = costs
                    cheapest_type = eventtype_to_acquire
                    best_single_sel = used_single_sel
                
                elif costs == min_costs and len(eventtype_to_acquire) > len(cheapest_type):
                    min_costs = costs
                    cheapest_type = eventtype_to_acquire

            
            total_costs += min_costs
            
            for event_type in self.determine_all_primitive_events_of_projection(cheapest_type):
                if event_type not in acquired_eventtypes:
                    acquired_eventtypes.append(event_type)
            
            plan.append([cheapest_type])
            used_types.append(str(best_single_sel))
            projection_to_process.primitive_operators.remove(cheapest_type)
        
        self.source_sent_this_type_to_node = copy.deepcopy(old_source_sent_this_type_to_node_map)
        return plan, used_types



    def get_push_costs(self, projection_to_process, node):
        push_costs = 0
        for eventtype_to_acquire in projection_to_process:
            
            number_of_sources = self.determine_correct_number_of_sources(node, eventtype_to_acquire)
            push_costs += self.outputrate_map[eventtype_to_acquire] * number_of_sources

        return push_costs

    def get_push_plan(self, eventtypes_to_acquire):
        push_plan = []
        for eventtype in eventtypes_to_acquire:
            
            push_plan.append(eventtype)

        return [push_plan]


    def determine_costs_for_greedy_plans_projection_on_node(self, plan, used_types, projection_to_process, node):
        push = True
        costs = 0
        available_predicates = []
        used_eventtype_to_pull = []
        used_eventtype = ''
        old_source_sent_this_type_to_node_map = copy.deepcopy(self.source_sent_this_type_to_node)
        projection_to_process = self.remove_locally_produced_eventtype_from_plan_if_multi_sink_placement_greedy(projection_to_process)
        push_plan = self.get_push_plan(projection_to_process.primitive_operators)
        push_plan_costs = self.get_push_costs(projection_to_process.primitive_operators, node)
        
        self.source_sent_this_type_to_node = copy.deepcopy(old_source_sent_this_type_to_node_map)
        current_plan_step = 0

        for eventtype_group in plan:
            for eventtype in eventtype_group:
                
                if push:                 
                    number_of_sources = self.determine_correct_number_of_sources(node, eventtype)
                    costs += self.outputrate_map[eventtype] * number_of_sources
                else:                    
                    pull_request_size = 0
                    pull_request_size = self.determine_optimized_pull_request_size_for_step(available_predicates, used_types[current_plan_step], node)
                    
                    pull_answer_size = 0
                    pull_answer_size = self.determine_optimized_pull_answer_size_for_step(used_types[current_plan_step], eventtype,node)

                    number_of_nodes = self.determine_correct_number_of_sources(node, eventtype)
                    
                    costs += ((pull_request_size / self.number_of_nodes_producing_this_projection) + pull_answer_size) * number_of_nodes
                    
            used_eventtype_to_pull.append(used_eventtype)
            for eventtype in eventtype_group:
                for prim_event in self.get_sorted_primitive_eventtypes_from_projection_string(eventtype):
                    available_predicates.append(prim_event)
            current_plan_step += 1
            push = False
       
        if costs < push_plan_costs:
            return costs, plan
        else:
            return push_plan_costs, push_plan


 


    ####################################################################################

        
    #multiple functions to parse permutations of numbers to permutations of characters

    #https://stackoverflow.com/questions/41588077/how-to-get-all-sorted-permutations-of-a-list-in-python
    #returns all permutations of size 2 ((n^2 - n)/2) for a given input (list of chars)
    def determine_permutation_pairs(self,events):
        unique = filter(lambda x: operator.le(x[0], x[1]), itertools.permutations(events, 2))
        return ([(str(a)+str(b)) for a, b in sorted(unique)])


    def numbers_to_events(self,numbers):
        return [(str(chr(ord(pair[0])+16))+str(chr(ord(pair[1])+16))) for pair in numbers]


    def events_to_numbers(self,events):
        return [chr(ord(event)-16) for event in events]


    def determine_event_permutations_pairs(self,events):
        numbers = self.events_to_numbers(events)
        permutations = self.determine_permutation_pairs(numbers)
        return self.numbers_to_events(permutations)


    #returns list of all primitive events for a given projection
    def determine_all_primitive_events_of_projection(self, projection):
        given_predicates = projection.replace('AND','')
        given_predicates = given_predicates.replace('SEQ','')
        given_predicates = given_predicates.replace('(','')
        given_predicates = given_predicates.replace(')','')
        given_predicates = re.sub(r'[0-9]+', '', given_predicates)
        given_predicates = given_predicates.replace(' ','')
        return given_predicates.split(',')



    #is the given only placed on one node? -> single sink placement
    def is_single_sink_placement(self,query):
        return len(query.node_placement)==1


    #memoize for faster calculations
    def determine_selectivity_map(self):
        #get all 2^n~ subsets of all primitive eventtypes
        for pair in self.all_combinations:
            relevant_permutations = self.determine_event_permutations_pairs(pair)

            selectivity_value = 1
            for relevant_permutation in relevant_permutations:
                selectivity_value *= self.selectivity_map[relevant_permutation]

            self.selectivity_map[pair] = selectivity_value


    #returns the total outputrate for a given number of event types, respecting the number  of sources producing this type
    def determine_outputrate_of_eventtype_combination(self, eventtypes):
        outputrate = 1
        for eventtype in eventtypes:
            outputrate *= self.outputrate_map[eventtype] * len(self.eventtype_to_sources_map[eventtype])

        return outputrate



    #returns a sorted key in order to access data within the hash tables (only memoize a result once, since
    #multiplication is commutative)
    def get_sorted_primitive_eventtypes_from_projection_string(self, new_eventtypes):
        eventtypes = ''

        if not isinstance(new_eventtypes, list):
            new_eventtypes = [new_eventtypes]

        for new_eventtype in new_eventtypes:
            if '(' in new_eventtype:
                if isinstance(new_eventtype, list):
                    eventtypes+= ''.join(sorted(self.determine_all_primitive_events_of_projection(new_eventtype[0])))
                else:
                    eventtypes+= ''.join(sorted(self.determine_all_primitive_events_of_projection(new_eventtype)))
            else:
                eventtypes += ''.join(sorted(new_eventtype))


        return ''.join(sorted(eventtypes))


    def remove_duplicates_and_sort_key(self, key):
        key = ''.join(dict.fromkeys(key))
        return ''.join(sorted(key))


    def determine_total_output_rate_of_eventtypes(self, eventtypes):
        eventtypes = self.remove_duplicates_and_sort_key(''.join(eventtypes))
        #total selectivity
        return self.determine_outputrate_of_eventtype_combination(eventtypes) * self.selectivity_map[eventtypes]
        

    def determine_optimized_pull_request_size_for_step(self, acquired_eventtypes, new_eventtypes, node):
        acquired_eventtypes = self.get_sorted_primitive_eventtypes_from_projection_string(acquired_eventtypes)
        eventtypes_to_pull_with = self.get_sorted_primitive_eventtypes_from_projection_string(new_eventtypes)

        minimized_pull_request = 0
        totally_combined_result = self.determine_total_output_rate_of_eventtypes(acquired_eventtypes)

        for eventtype_to_pull_with in eventtypes_to_pull_with:
            if len(acquired_eventtypes) == 1:
                if node in self.eventtype_to_sources_map[eventtype_to_pull_with]:
                    number_of_sources = len(self.eventtype_to_sources_map[eventtype_to_pull_with]) - 1
                else:
                    number_of_sources = len(self.eventtype_to_sources_map[eventtype_to_pull_with])
                minimized_pull_request += self.outputrate_map[eventtype_to_pull_with] * number_of_sources 
            else:
                single_selectivity_key = str(eventtype_to_pull_with) + '|' + self.remove_duplicates_and_sort_key(acquired_eventtypes)
                if node in self.eventtype_to_sources_map[eventtype_to_pull_with]:
                    number_of_sources = len(self.eventtype_to_sources_map[eventtype_to_pull_with]) - 1
                else:
                    number_of_sources = len(self.eventtype_to_sources_map[eventtype_to_pull_with])

                minimized_pull_request += self.outputrate_map[eventtype_to_pull_with] * self.single_selectivity_of_eventtype_within_projection[single_selectivity_key] * number_of_sources 


        if minimized_pull_request < totally_combined_result:
            return minimized_pull_request
        else:
            return minimized_pull_request      


    def determine_optimized_pull_answer_size_for_step(self, transfered_eventtypes, next_eventtype_to_pull, node):
        sorted_transfered_eventtypes_key = self.get_sorted_primitive_eventtypes_from_projection_string(transfered_eventtypes)
        next_eventtype_to_pull_key = self.get_sorted_primitive_eventtypes_from_projection_string(next_eventtype_to_pull)
        sorted_all_eventtypes_key = sorted_transfered_eventtypes_key + next_eventtype_to_pull_key
        sorted_all_eventtypes_key = self.remove_duplicates_and_sort_key(sorted_all_eventtypes_key)
        
        optimized_pull_answer = 0
        for next_eventtype in next_eventtype_to_pull_key:

            single_selectivity_key =  str(next_eventtype) + '|' + str(sorted_all_eventtypes_key)
            if node in self.eventtype_to_sources_map[next_eventtype]:
                number_of_sources = len(self.eventtype_to_sources_map[next_eventtype]) - 1
            else:
                number_of_sources = len(self.eventtype_to_sources_map[next_eventtype])
                
            optimized_pull_answer += self.outputrate_map[next_eventtype] * self.single_selectivity_of_eventtype_within_projection[single_selectivity_key] * number_of_sources 
        
        totally_combined_result = self.determine_total_output_rate_of_eventtypes(sorted_all_eventtypes_key)
        if optimized_pull_answer < totally_combined_result:
            return optimized_pull_answer
        else:   
            return optimized_pull_answer
    


    def determine_correct_number_of_sources(self,node,eventtype):        
        number_of_sources = 0
        
        for source in self.eventtype_to_sources_map[eventtype]:
            key = str(source) +"~"+ str(node)+ "~" + str(eventtype)
            if key not in self.source_sent_this_type_to_node and source is not node:
                number_of_sources += 1
                #self.source_sent_this_type_to_node[key] = True
        
        return number_of_sources


    

    def determine_optimal_pull_strategy_for_step_in_plan(self, acquired_eventtypes, eventtype_to_acquire, node):
        acquired_eventtypes = self.get_sorted_primitive_eventtypes_from_projection_string(acquired_eventtypes)

        #### Pull step caching ###
        key = (acquired_eventtypes, eventtype_to_acquire, node)
        if key in self.optimal_pull_strategy_cache:
            return self.optimal_pull_strategy_cache[key].lowest_costs, self.optimal_pull_strategy_cache[key].best_step



        all_permutations = determine_permutations_of_all_relevant_lengths(acquired_eventtypes, 1, len(acquired_eventtypes))
        lowest_costs_for_step = float('inf')
        best_step = ''
        
        old_source_sent_this_type_to_node_map = copy.deepcopy(self.source_sent_this_type_to_node)

        for events_to_pull_with in all_permutations:    
            pull_request_size = self.determine_optimized_pull_request_size_for_step(acquired_eventtypes, events_to_pull_with, node)
            pull_answer_size = self.determine_optimized_pull_answer_size_for_step(events_to_pull_with, eventtype_to_acquire, node)

            total_costs_for_step = ((pull_request_size / self.number_of_nodes_producing_this_projection) + pull_answer_size) * self.determine_correct_number_of_sources(node, eventtype_to_acquire)
            if total_costs_for_step < lowest_costs_for_step:# and total_costs_for_step > 0:
                lowest_costs_for_step = total_costs_for_step
                best_step = events_to_pull_with

            self.source_sent_this_type_to_node = copy.deepcopy(old_source_sent_this_type_to_node_map)


        optimal_push_pull_decision = CachedOptimalStep(lowest_costs_for_step, best_step)
        self.optimal_pull_strategy_cache[key] = optimal_push_pull_decision
            
        return lowest_costs_for_step, best_step


    def determine_costs_of_push_pull_plan(self,plan,projection_to_process, node = 1337):
        push = True
        costs = 0

        available_predicates = []
             
        for eventtype_group in plan:
            for eventtype in eventtype_group:
                if push:
                    costs += self.outputrate_map[eventtype] * self.determine_correct_number_of_sources(node, eventtype)
                else:
                    lowest_costs_for_this_step, used_eventtypes = self.determine_optimal_pull_strategy_for_step_in_plan(available_predicates, eventtype, node)
                    costs += lowest_costs_for_this_step

            for eventtype in eventtype_group:
                available_predicates.append(eventtype)
            
            push = False
    
        return costs


    def create_combined_key(self, eventtype, projection_to_process):
        combined_key = ''
        if len(eventtype) > 1:
            primitive_events = self.determine_all_primitive_events_of_projection(eventtype)
            for primitive_event in primitive_events:
                if primitive_event is not projection_to_process.forbidden_event_types:
                    combined_key += str(primitive_event)
        else:
            if eventtype is not projection_to_process.forbidden_event_types and len(projection_to_process.node_placement) > 1:
                combined_key += str(eventtype)

        return combined_key


    def determine_costs_for_projection_on_node(self, plan, projection_to_process, node):
        push = True
        costs = 0

        old_source_sent_this_type_to_node_map = copy.deepcopy(self.source_sent_this_type_to_node)
        push_plan_costs = self.get_push_costs(projection_to_process.primitive_operators, node)
        self.source_sent_this_type_to_node = copy.deepcopy(old_source_sent_this_type_to_node_map)
        
        available_predicates = []
        used_eventtype_to_pull = []
        used_eventtype = ''
        for eventtype_group in plan:
            used_eventtypes = []
            for eventtype in eventtype_group:
                if push:                 
                    number_of_sources = self.determine_correct_number_of_sources(node, eventtype)
                    costs += self.outputrate_map[eventtype] * number_of_sources
                else:
                    lowest_costs_for_this_step, used_eventtype = self.determine_optimal_pull_strategy_for_step_in_plan(available_predicates, eventtype, node)

                    costs += lowest_costs_for_this_step
                    for used_type in used_eventtype:
                        used_eventtypes.append(used_type)

            used_eventtype_to_pull.append(list(set(used_eventtypes)))
                    
            for eventtype in eventtype_group:
                for prim_event in self.get_sorted_primitive_eventtypes_from_projection_string(eventtype):
                    available_predicates.append(prim_event)
                
            push = False
        
        if costs < push_plan_costs:
            return costs, used_eventtype_to_pull
        else:
            return push_plan_costs, [[]]


    #https://stackoverflow.com/questions/32694444/algorithm-to-generate-all-preorders-weak-orders-of-size-n
    #these weakorders represent the ordered bell number and equal exactly the number of possible push-pull plans
    #therefore create all weakorders of a given list of events and parse them for evaluation
    def weakorders(self,A):
        if not A:  # i.e., A is empty
            yield []
            return
        for k in range(1, len(A) + 1):
            for B in itertools.combinations(A, k):  # i.e., all nonempty subsets B
                for order in self.weakorders(set(A) - set(B)):
                    yield [B] + order


    #parses all weakorders into a format of lists of lists, instead of sets
    def weak_ordered_plans_generator(self, eventtypes_to_match_projection):
        tmp = []
        for plan in self.weakorders(eventtypes_to_match_projection):
            for x in plan:
                tmp_list = list(x)
                for i in range(0,len(tmp_list)):
                    tmp_list[i] = self.single_eventtype_to_projection_map[tmp_list[i]]
                tmp.append(tmp_list)
            
            yield tmp
            tmp = []

    
    #For factorial plan enumeration#############
    def single_step_plan_permutations(self, A):
        for query in itertools.permutations(A):
            query = list(query)
            correct_query_format = [query[i:i+1] for i in range(0, len(query), 1)]
            
            yield correct_query_format

    
    def single_step_factorial_plan_generator(self, eventtypes_to_match_projection):
        tmp = []
        for plan in self.single_step_plan_permutations(eventtypes_to_match_projection):
            for x in plan:
                tmp_list = list(x)
                for i in range(0,len(tmp_list)):
                    tmp_list[i] = self.single_eventtype_to_projection_map[tmp_list[i]]
                tmp.append(tmp_list)
            
            yield tmp
            tmp = []
    ########################################


    #in order to create all weakorders containing arbitrary projections (e.g., A, SEQ(B,C), D,..)
    def initiate_mapping_from_projection_to_single_eventtype(self,eventtypes_to_match_projection):
        char_counter = 0
        for eventtype in eventtypes_to_match_projection:
            self.projection_to_single_eventtype_map[eventtype] = (chr(ord('A')+char_counter))
            self.single_eventtype_to_projection_map[(chr(ord('A')+char_counter))] = eventtype
            char_counter+=1

        char_counter = 0
        for eventtype in eventtypes_to_match_projection:
            eventtypes_to_match_projection[char_counter] = (chr(ord('A')+char_counter))
            char_counter+=1

    #dont use the LPE for pulling, since its outputrate does not pay off when used as pull request
    def remove_locally_produced_eventtype_from_plan_if_multi_sink_placement(self, projection_to_process):
        if not self.is_single_sink_placement(projection_to_process):
            for projection in projection_to_process.primitive_operators:
                if self.single_eventtype_to_projection_map[projection] == projection_to_process.forbidden_event_types:
                    projection_to_process.primitive_operators.remove(projection)
        
        return projection_to_process

    #modified from https://stackoverflow.com/questions/25458879/algorithm-to-produce-all-partitions-of-a-list-in-order/25460561
    def determine_all_single_step_plan_partitionings(self, best_plan):
        n = len(best_plan)
        for partition_index in range(2 ** (n-1)):

            # current partition, e.g., [['a', 'b'], ['c', 'd', 'e']]
            partition = []

            # used to accumulate the subsets, e.g., ['a', 'b']
            subset = []

            for position in range(n):

                subset.append(best_plan[position])

                # check whether to "break off" a new subset
                if 1 << position & partition_index or position == n-1:
                    partition.append(sorted(subset))
                    subset = []

            yield partition


    def single_step_factorial_plan_sampling_generator(self, s, eventtypes):
        counter = 0
        possible_ordering = []
        for eventtype_group in eventtypes:
            for eventtype in eventtype_group:
                possible_ordering.append(eventtype)
        for i in range(0,s):
            result = []
            random.shuffle(possible_ordering)
            for y in possible_ordering:
                next_eventtype = self.single_eventtype_to_projection_map[y]
                result.append([next_eventtype])

            yield result           
            
    
    
    def manage_top_k_plans(self, top_k_plans, plan , top_k):
        if len(top_k_plans) < top_k:
            top_k_plans.append(plan)
            return top_k_plans
        
        highest_costs_idx = 0
        highest_costs = 0
        curr_idx = 0
        for top_k_plan in top_k_plans:
            if top_k_plan.costs > highest_costs:
                highest_costs = top_k_plan.costs
                highest_costs_idx = curr_idx
            curr_idx += 1

        if highest_costs > plan.costs:
            del top_k_plans[highest_costs_idx]
            top_k_plans.append(plan)
            
        return top_k_plans


    def determine_approximated_factorial_push_pull_plan(self, projection_to_process, top_k, node):
        best_push_pull_plan = ""
        lowest_normal_costs = float('inf')
        top_k_plans = []
        top_k_plan_caching = {}
        
        self.initiate_mapping_from_projection_to_single_eventtype(projection_to_process.primitive_operators)
        old_source_sent_this_type_to_node_map = copy.deepcopy(self.source_sent_this_type_to_node)
        projection_to_process = self.remove_locally_produced_eventtype_from_plan_if_multi_sink_placement(projection_to_process)
        self.number_of_nodes_producing_this_projection = len(projection_to_process.node_placement)
        
        for plan in self.single_step_factorial_plan_generator(projection_to_process.primitive_operators):
            current_normal_costs = self.determine_costs_of_push_pull_plan(plan, projection_to_process, node)

            self.source_sent_this_type_to_node = copy.deepcopy(old_source_sent_this_type_to_node_map)
            top_k_plans = self.manage_top_k_plans(top_k_plans, PushPullPlan(plan,current_normal_costs), top_k)
            if lowest_normal_costs > current_normal_costs:
                best_push_pull_plan = plan
                lowest_normal_costs = current_normal_costs

        for top_k_plan in top_k_plans:
            tmp_plan = [element for sublist in top_k_plan.plan for element in sublist]
            for plan in self.determine_all_single_step_plan_partitionings(tmp_plan):
                key = ''
                for subgroup in plan:
                    sub_part = ''
                    for ele in subgroup:
                        sub_part += ele
                    sub_part = ''.join(sorted(sub_part))
                    sub_part += ','
                    key += sub_part
                    
                top_k_cache_key = (key, node)
                if top_k_cache_key in top_k_plan_caching:
                    current_normal_costs = top_k_plan_caching[top_k_cache_key]
                else:
                    self.source_sent_this_type_to_node = copy.deepcopy(old_source_sent_this_type_to_node_map)
                    current_normal_costs = self.determine_costs_of_push_pull_plan(plan, projection_to_process, node)
                    top_k_plan_caching[top_k_cache_key] = current_normal_costs
                
                self.source_sent_this_type_to_node = copy.deepcopy(old_source_sent_this_type_to_node_map)
                if lowest_normal_costs > current_normal_costs:
                    best_push_pull_plan = plan
                    lowest_normal_costs = current_normal_costs
        
        return best_push_pull_plan


    
    def determine_approximated_factorial_sampling_push_pull_plan(self, projection_to_process, top_k, sample_size, node):
        best_push_pull_plan = ""
        lowest_normal_costs = float('inf')
        top_k_plans = []
        top_k_plan_caching = {}
        
        self.initiate_mapping_from_projection_to_single_eventtype(projection_to_process.primitive_operators)
        old_source_sent_this_type_to_node_map = copy.deepcopy(self.source_sent_this_type_to_node)
        projection_to_process = self.remove_locally_produced_eventtype_from_plan_if_multi_sink_placement(projection_to_process)
        self.number_of_nodes_producing_this_projection = len(projection_to_process.node_placement)
        
        for plan in self.single_step_factorial_plan_sampling_generator(sample_size, projection_to_process.primitive_operators):
            current_normal_costs = self.determine_costs_of_push_pull_plan(plan, projection_to_process, node)
                
            self.source_sent_this_type_to_node = copy.deepcopy(old_source_sent_this_type_to_node_map)
            top_k_plans = self.manage_top_k_plans(top_k_plans, PushPullPlan(plan,current_normal_costs), top_k)
            if lowest_normal_costs > current_normal_costs:
                best_push_pull_plan = plan
                lowest_normal_costs = current_normal_costs

        for top_k_plan in top_k_plans:
            tmp_plan = [element for sublist in top_k_plan.plan for element in sublist]
            for plan in self.determine_all_single_step_plan_partitionings(tmp_plan):
                key = ''
                for subgroup in plan:
                    sub_part = ''
                    for ele in subgroup:
                        sub_part += ele
                    sub_part = ''.join(sorted(sub_part))
                    sub_part += ','
                    key += sub_part
                    
                top_k_cache_key = (key, node)
                if top_k_cache_key in top_k_plan_caching:
                    current_normal_costs = top_k_plan_caching[top_k_cache_key]
                else:
                    self.source_sent_this_type_to_node = copy.deepcopy(old_source_sent_this_type_to_node_map)    
                    current_normal_costs = self.determine_costs_of_push_pull_plan(plan, projection_to_process,node)
                    top_k_plan_caching[top_k_cache_key] = current_normal_costs
                
                self.source_sent_this_type_to_node = copy.deepcopy(old_source_sent_this_type_to_node_map)
                if lowest_normal_costs > current_normal_costs:
                    best_push_pull_plan = plan
                    lowest_normal_costs = current_normal_costs

        return best_push_pull_plan


        


    
    def determine_exact_push_pull_plan(self, projection_to_process, node):
        best_push_pull_plan = ""
        lowest_normal_costs = float('inf')

        self.initiate_mapping_from_projection_to_single_eventtype(projection_to_process.primitive_operators)
        
        old_source_sent_this_type_to_node_map = copy.deepcopy(self.source_sent_this_type_to_node)
        
        projection_to_process = self.remove_locally_produced_eventtype_from_plan_if_multi_sink_placement(projection_to_process)
        self.number_of_nodes_producing_this_projection = len(projection_to_process.node_placement)
        
        for plan in self.weak_ordered_plans_generator(projection_to_process.primitive_operators):
            current_normal_costs = self.determine_costs_of_push_pull_plan(plan, projection_to_process, node)
            
            self.source_sent_this_type_to_node = copy.deepcopy(old_source_sent_this_type_to_node_map)
            if lowest_normal_costs > current_normal_costs:
                best_push_pull_plan = plan
                lowest_normal_costs = current_normal_costs
        
        
        return best_push_pull_plan


