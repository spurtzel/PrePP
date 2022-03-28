import re

import math

from timeit import default_timer as timer

import random

import copy

import push_pull_plan_generator

import sys

import os

from itertools import permutations, chain, combinations

NETWORK = 'network'
QUERIES = 'queries'
MUSE_GRAPH = 'muse graph'
SELECTIVITIES = 'selectivities'

CURRENT_SECTION = ''

network = []

queries_to_process = []

query_network = []

eventtype_pair_to_selectivity = {}

eventtypes_single_selectivities = {}

single_selectivity_of_eventtype_within_projection = {}

projection_dependencies_map = {}

all_event_combinations = []

all_eventtype_output_rates = {}

eventtype_to_sources_map = {}

eventtype_to_nodes = {}


class Query_fragment():
    def __init__(self,query, primitive_operators, node_placement, forbidden_event_types):
        self.query = query
        self.primitive_operators = primitive_operators
        self.node_placement = node_placement
        self.forbidden_event_types = forbidden_event_types


def get_current_section(line):
    if line == 'network\n':
        return NETWORK
    elif line == 'queries\n':
        return QUERIES
    elif line == 'muse graph\n':
        return MUSE_GRAPH
    elif line == 'selectivities\n':
        return SELECTIVITIES
    return CURRENT_SECTION


def extract_network_node(line):
    if line.find('[')!=-1:
        return list(map(int, line[line.find('[')+1:line.find(']')].split(", ")))


def extract_node_events_produced(output_rates, current_node):
    if output_rates is None:
        return 0
    char_counter = 0
    eventtypes_produced = []
    for output_rate in output_rates:
        if output_rate > 0:
            all_eventtype_output_rates[(chr(ord('A')+char_counter))] = output_rate

            if chr(ord('A')+char_counter) not in eventtype_to_sources_map:
                eventtype_to_sources_map[chr(ord('A')+char_counter)] = []
                eventtype_to_sources_map[chr(ord('A')+char_counter)].append(current_node)
            else:
                eventtype_to_sources_map[chr(ord('A')+char_counter)].append(current_node)

            eventtypes_produced.append(chr(ord('A')+char_counter))    
        char_counter +=1
        
    return eventtypes_produced


#AND(B1, SEQ(F, AND(G1, SEQ(G2, AND(I1, I2, B2)))))
def get_all_query_components(query):
    operators = ['AND','SEQ']

    #adjust query format -> no digits/whitespaces
    query = re.sub(r'[0-9]+', '', query)
    query = query.replace(' ','')

    #current_pos += 4 jumps over the first L_PAREN -> 1
    open_parentheses = 1
    current_pos = 4
    query_components = []
    
    for idx in range(current_pos, len(query)):

        if query[current_pos:current_pos+3] in operators:
            first_pos = current_pos
            
            #set to pos x in AND(x / SEQ(x
            current_pos += 4
            while open_parentheses >= 1:
                if query[current_pos:current_pos+3] in operators:
                    current_pos += 3

                if query[current_pos] == '(':
                    open_parentheses += 1

                if query[current_pos] == ')':
                    open_parentheses -= 1

                current_pos +=1

            eventtype = query[first_pos:current_pos]
            eventtype = re.sub(r'[0-9]+', '', eventtype)
            query_components.append(eventtype)
        else:
            if current_pos+1 < len(query):
                if query[current_pos].isalpha():
                    query_components.append(query[current_pos])
        
        current_pos +=1

    return query_components


def is_complex_eventtype(eventtype):
    return len(eventtype) > 1

def determine_query_output_rate(query, multi_sink_placement_eventtype, is_single_sink_placement):
    query = re.sub(r'[0-9]+', '', query)
    
    if not is_complex_eventtype(query):
        if query != multi_sink_placement_eventtype or is_single_sink_placement:
            return all_eventtype_output_rates[query] * len(eventtype_to_sources_map[query])
        else:
            return all_eventtype_output_rates[query]
    
    output_rate = 1
    first_operator = query[0:3]
    all_query_components = get_all_query_components(query)
    for eventtype in all_query_components:
        output_rate *= determine_query_output_rate(eventtype, multi_sink_placement_eventtype, is_single_sink_placement)

    if first_operator == 'SEQ':
        return output_rate

    if first_operator == 'AND':
        operand_count = len(all_query_components)
        return  operand_count * output_rate


def determine_all_primitive_events_of_projection(projection):
    given_predicates = projection.replace('AND','')
    given_predicates = given_predicates.replace('SEQ','')
    given_predicates = given_predicates.replace('(','')
    given_predicates = given_predicates.replace(')','')
    given_predicates = re.sub(r'[0-9]+', '', given_predicates)
    given_predicates = given_predicates.replace(' ','')
    return given_predicates.split(',')


def determine_total_query_selectivity(query):
    selectivity = 1.0
    
    for i in range(0,len(query)-1):
        for k in range(i+1,len(query)):
            selectivity *= float(eventtype_pair_to_selectivity[str(query[i]) + str(query[k])])
    
    return selectivity 

 
def determine_total_query_outputrate(query):
    outputrate = 1.0
    
    for eventtype in query:
        outputrate *= all_eventtype_output_rates[eventtype] * len(eventtype_to_sources_map[eventtype])

    
    return outputrate 

def determine_query_selectivity(query):
    query = query.replace('AND','')
    query = query.replace('SEQ','')
    query = query.replace('(','')
    query = query.replace(')','')
    query = re.sub(r'[0-9]+', '', query)
    query = query.replace(' ','')

    primitive_events = query.split(',')

    selectivity = 1.0
    
    for i in range(0,len(primitive_events)-1):
        for k in range(i+1,len(primitive_events)):
            selectivity *= float(eventtype_pair_to_selectivity[str(primitive_events[i]) + str(primitive_events[k])])
    
    return selectivity 



def determine_total_query_rate(query):
    return determine_query_output_rate(query.query, query.forbidden_event_types, is_single_sink_placement(query))*determine_query_selectivity(query.query)



def extract_queries(line):
    if line != "queries" and len(line)>1:
        queries_to_process.append(line[0:len(line)-1])
        return line

    
def extract_muse_graph_queries(line):
    if line.find('SELECT')!=-1:
        return line[line.find('SELECT')+7:line.find('FROM')-1]


def extract_muse_graph_sub_queries(line):
    if line.find('FROM')!=-1:
        return line[line.find('FROM')+5:line.find('ON')-1].split("; ")


def extract_muse_graph_sources(line):
    if line.find('{')!=-1:
        return list(map(int, line[line.find('{')+1:line.find('}')].split(", ")))


def extract_muse_graph_forbidden(line):
    if line.find('/n(')!=-1:
        if line.find('WITH')!=-1:
            return line[line.find('/n(')+3:line.find('WITH')-2]
        else:
            return line[line.find('/n(')+3:len(line)-2]


def extract_muse_graph_selectivities(line):
    all_positions_of_eventcombinations = [m.start() for m in re.finditer("'", line)]
    all_positions_of_eventproducts = [m.start() for m in re.finditer(",", line)]
    all_positions_of_eventproducts = all_positions_of_eventproducts + [m.start() for m in re.finditer("}", line)]
    
    index_adjust = 0
    for index in range(0,len(all_positions_of_eventproducts)):
        first_product_index = all_positions_of_eventcombinations[0 + index_adjust]
        second_product_index = all_positions_of_eventcombinations[1 + index_adjust]

        event_product = line[first_product_index+1:second_product_index]
        
        if (index_adjust//2)%2==0:
            all_event_combinations.append(event_product)
            
        first_selectivity_index = all_positions_of_eventcombinations[1 + index_adjust]
        second_selectivity_index = all_positions_of_eventproducts[index]

        event_selectivity = line[first_selectivity_index+3:second_selectivity_index]
        
        eventtype_pair_to_selectivity[event_product] = float(event_selectivity)
        eventtype_pair_to_selectivity[2*str(event_product[0])] = 1
        index_adjust += 2
    
  
def is_single_sink_placement(query):
    return len(query.node_placement) == 1



def determine_randomized_distribution_push_pull_costs(queries, eventtype_combinations, highest_primitive_eventtype_to_be_processed, algorithm, samples, k, plan_print):
    total_costs_before = 0
    all_costs = []

    from_projection_to_best_push_pull_plan = {}

    total_greedy_costs = 0
    total_exact_costs = 0
    total_factorial_costs = 0
    total_sampling_costs = 0
    
    push_pull_plan_generator_greedy = push_pull_plan_generator.Initiate(eventtype_pair_to_selectivity, eventtype_to_sources_map, all_eventtype_output_rates, eventtypes_single_selectivities, single_selectivity_of_eventtype_within_projection, eventtype_combinations, highest_primitive_eventtype_to_be_processed)
    
    push_pull_plan_generator_exact = push_pull_plan_generator.Initiate(eventtype_pair_to_selectivity, eventtype_to_sources_map, all_eventtype_output_rates, eventtypes_single_selectivities, single_selectivity_of_eventtype_within_projection, eventtype_combinations, highest_primitive_eventtype_to_be_processed)
    
    push_pull_plan_generator_factorial = push_pull_plan_generator.Initiate(eventtype_pair_to_selectivity, eventtype_to_sources_map, all_eventtype_output_rates, eventtypes_single_selectivities, single_selectivity_of_eventtype_within_projection, eventtype_combinations, highest_primitive_eventtype_to_be_processed)
    
    push_pull_plan_generator_sampling = push_pull_plan_generator.Initiate(eventtype_pair_to_selectivity, eventtype_to_sources_map, all_eventtype_output_rates, eventtypes_single_selectivities, single_selectivity_of_eventtype_within_projection, eventtype_combinations, highest_primitive_eventtype_to_be_processed)

    greedy_exec_times = []
    exact_exec_times = []
    factorial_exec_times = []
    sampling_exec_times = []
    for query in queries:
        if query.query == '':
            continue
        old_copy = copy.deepcopy(query.primitive_operators)
        top_k = int(k)

        for current_node in query.node_placement:
            #print("~~~~~~~~")
            query.primitive_operators = copy.deepcopy(old_copy)
            if algorithm == "g":
                start_greedy = timer()
                greedy_single_step_push_pull_plan_for_a_projection, used_pull_eventtypes = push_pull_plan_generator_greedy.greedy_single_step_plan_generator(query, current_node)
                
                
                end_greedy = timer()
                greedy_exec_times.append(end_greedy-start_greedy)

                push_pull_plan_generator_exact.source_sent_this_type_to_node =  copy.deepcopy(push_pull_plan_generator_greedy.source_sent_this_type_to_node)
                query.primitive_operators = copy.deepcopy(old_copy)
                greedy_costs, used_eventtypes_to_pull = push_pull_plan_generator_greedy.determine_costs_for_greedy_plans_projection_on_node(greedy_single_step_push_pull_plan_for_a_projection, used_pull_eventtypes, query, current_node)
                if plan_print == "t":
                    if len(used_eventtypes_to_pull) == 1:
                        print("greedy_single_step_push_pull_plan_for_a_projection:", used_eventtypes_to_pull)
                        print("used_eventtypes_to_pull:", used_eventtypes_to_pull)
                    else:
                        print("greedy_single_step_push_pull_plan_for_a_projection:", greedy_single_step_push_pull_plan_for_a_projection)
                        print("used_eventtypes_to_pull:", used_eventtypes_to_pull)
                total_greedy_costs += greedy_costs

            query.primitive_operators = copy.deepcopy(old_copy)
            sample_size = int(samples)

            if algorithm == "s":
                start_sampling = timer()
                best_approximated_sampling_push_pull_plan_for_a_projection = push_pull_plan_generator_sampling.determine_approximated_factorial_sampling_push_pull_plan(query, top_k, sample_size, current_node)
                
                end_sampling = timer()
                sampling_exec_times.append(end_sampling-start_sampling)
                
                query.primitive_operators = copy.deepcopy(old_copy)
                sampling_costs, used_eventtypes_to_pull = push_pull_plan_generator_sampling.determine_costs_for_projection_on_node(best_approximated_sampling_push_pull_plan_for_a_projection, query, current_node)
                if plan_print == "t":
                    print("best_sampling_prepp_plan_for_a_projection:", best_approximated_sampling_push_pull_plan_for_a_projection)
                    print("used_eventtypes_to_pull:", used_eventtypes_to_pull)
                total_sampling_costs += sampling_costs
            query.primitive_operators = copy.deepcopy(old_copy)

            if algorithm == "f":
                start_factorial = timer()
                best_factorial_push_pull_plan_for_a_projection = push_pull_plan_generator_factorial.determine_approximated_factorial_push_pull_plan(query, top_k, current_node)
                
                end_factorial = timer()
                factorial_exec_times.append(end_factorial-start_factorial)

                query.primitive_operators = copy.deepcopy(old_copy)
                factorial_costs, used_eventtypes_to_pull = push_pull_plan_generator_factorial.determine_costs_for_projection_on_node(best_factorial_push_pull_plan_for_a_projection, query, current_node)
                if plan_print == "t":
                    print("best_factorial_push_pull_plan_for_a_projection:", best_factorial_push_pull_plan_for_a_projection)
                    print("used_eventtypes_to_pull:", used_eventtypes_to_pull)
                total_factorial_costs += factorial_costs
            query.primitive_operators = copy.deepcopy(old_copy)


            if algorithm == "e":
                start_exact = timer()
                exact_push_pull_plan_for_a_projection = push_pull_plan_generator_exact.determine_exact_push_pull_plan(query, current_node)
                
                end_exact = timer()
                exact_exec_times.append(end_exact-start_exact)
                query.primitive_operators = copy.deepcopy(old_copy)
                exact_costs, used_eventtypes_to_pull = push_pull_plan_generator_exact.determine_costs_for_projection_on_node(exact_push_pull_plan_for_a_projection, query, current_node)
                if plan_print == "t":
                    print("exact_push_pull_plan_for_a_projection:", exact_push_pull_plan_for_a_projection)
                    print("used_eventtypes_to_pull:", used_eventtypes_to_pull)
                total_exact_costs += exact_costs



    return total_greedy_costs, total_sampling_costs, total_factorial_costs, total_exact_costs, sum(greedy_exec_times), sum(exact_exec_times), sum(factorial_exec_times), sum(sampling_exec_times)



#return the current upper bound for a given eventtype based on all possible lower bounds of size n-1
def return_minimum_upper_bound(upper_bounds, eventtype):
    lowest_upper_bound = 1.0

    for _list in upper_bounds:
        for ele in _list:
            if ele == eventtype:
                key = str(eventtype) + '|' + str(_list)

                if key in single_selectivity_of_eventtype_within_projection:
                    if lowest_upper_bound > single_selectivity_of_eventtype_within_projection[key]:
                        lowest_upper_bound = single_selectivity_of_eventtype_within_projection[key]
    return lowest_upper_bound
                


def no_better_option_found_handling(query, upper_bounds_keys):
    for idx in range(0,len(query)):
        upper_bound = return_minimum_upper_bound(upper_bounds_keys, query[idx])
        key = str(query[idx]) + '|' + str(query)
        single_selectivity_of_eventtype_within_projection[key] = upper_bound



def determine_randomized_single_selectivities_within_all_projections(query, upper_bounds_keys):  
    projection_selectivity = determine_total_query_selectivity(query)
    projection_outputrate = determine_total_query_outputrate(query)
    total_outputrate = projection_outputrate * projection_selectivity
    
    outputrates = []
    for primitive_eventtype in query:
        outputrates.append((all_eventtype_output_rates[primitive_eventtype] * len(eventtype_to_sources_map[primitive_eventtype])))
    
    limit = len(query)

    solution_found = False
    total_sel = projection_selectivity


    delta = 0
    decreasing_value = 1
    while not solution_found:
        delta += 1
        first_n_random_values = []
        product = 1

        current_idx = 0
        chosen_indices = [ele for ele in range(0,limit)]
        random.shuffle(chosen_indices)

        for n in range(0,len(chosen_indices)-1):
            if delta == 2000:
                no_better_option_found_handling(query, upper_bounds_keys)
                return

            lower_bound = total_sel
            upper_bound = return_minimum_upper_bound(upper_bounds_keys, query[chosen_indices[n]])
            
            first_n_random_values.append(random.uniform(lower_bound, upper_bound))
            product *= first_n_random_values[n]
        
        if total_sel/product <= 1.0:
            solution_found = True
            first_n_random_values.append(total_sel/product)

            idx = 0

            for random_value in first_n_random_values:
                if total_outputrate > 1.0:
                    if (random_value * outputrates[chosen_indices[idx]] < 1.0 and projection_outputrate > 1.0) or (random_value * outputrates[chosen_indices[idx]]) > projection_outputrate:
                        solution_found = False
                        break
                else:
                    if (random_value * outputrates[chosen_indices[idx]]) > projection_outputrate:
                        solution_found = False
                        break
                idx += 1


    idx = 0
    for random_value in first_n_random_values:
        projection_key = str(query[chosen_indices[idx]]) + '|' + str(query)
        single_selectivity_of_eventtype_within_projection[projection_key] = first_n_random_values[idx]
        idx +=1



def determine_permutations_of_all_relevant_lengths(eventtypes, start_length = 2, end_length = 7):
    "[A,B,C] --> [], [A], [B], [C], [A,B], [A,C], [B,C], [A,B,C]"
    result = []
    for current_subset in chain.from_iterable(combinations(eventtypes, it) for it in range(start_length, end_length+1)):
        result.append(''.join(current_subset))
        
    return result


def determine_next_smaller_dependencies(eventtypes):
    "[A,B,C] --> [A,B], [A,C], [B,C]"
    result = []
    for current_subset in chain.from_iterable(combinations(eventtypes, it) for it in range(len(eventtypes), len(eventtypes)+1)):
        result.append(''.join(current_subset))
        
    return result

def get_all_distinct_eventtypes_of_used_queries_and_largest_query():
    total_list = []
    biggest_query_length = 0
    for query in queries_to_process:
        _list = determine_all_primitive_events_of_projection(query)
        if biggest_query_length < len(_list):
            biggest_query_length = len(_list)
        for _item in _list:
            if _item not in total_list:
                total_list.append(_item)
    #print(total_list)
    return list(''.join(sorted(total_list))), biggest_query_length


def determine_all_single_selectivities_for_every_possible_projection():
    all_needed_eventtypes, max_needed_query_length = get_all_distinct_eventtypes_of_used_queries_and_largest_query()
    
    all_possible_projections = determine_permutations_of_all_relevant_lengths(all_needed_eventtypes, 2, max_needed_query_length+1)

    for eventtype in all_needed_eventtypes:
        single_selectivity_of_eventtype_within_projection[eventtype] = 1.0
    
    all_different_projection_lengths = []

    current_length = 2
    current_length_projections = []
    for possible_projection in all_possible_projections:
        if len(possible_projection) > current_length:
            all_different_projection_lengths.append(current_length_projections)
            current_length_projections = []
            current_length += 1
            current_length_projections.append(possible_projection)
        else:
            current_length_projections.append(possible_projection)
        
    all_different_projection_lengths.append([all_possible_projections[len(all_possible_projections)-1]])
    
    for current_length_projections in all_different_projection_lengths:
        for projection in current_length_projections:
            upper_bound_keys = []
            if len(projection) > 2:
                upper_bound_keys = determine_next_smaller_dependencies(projection)
                
            determine_randomized_single_selectivities_within_all_projections(projection, upper_bound_keys)


    


if __name__ == "__main__":
    #os.nice(19)

    method = sys.argv[2]
    algorithm = sys.argv[3]
    samples = sys.argv[4]
    topk = sys.argv[5]
    runs = sys.argv[6]
    plan_print = sys.argv[7]

    input_file_name = sys.argv[1]
    input_file = open(input_file_name+".txt", "r")
    single_sink_evaluation_node = []
    single_sink_query_network = []
    current_node = 0
    current_highest = 0
    total_sum = 0
    for line in input_file:
        OLD_SECTION = CURRENT_SECTION
        CURRENT_SECTION = get_current_section(line)
        if OLD_SECTION != CURRENT_SECTION:
            continue
        
        if CURRENT_SECTION == NETWORK:
            if not "[" in line:
                continue
            output_rates = extract_network_node(line)
            
            if sum(output_rates) > current_highest:
                single_sink_evaluation_node = current_node
                current_highest = sum(output_rates)
            result = extract_node_events_produced(output_rates, current_node)
            current_node += 1
            if result != 0:
                network.append(result)

        if CURRENT_SECTION == QUERIES:
            extract_queries(line)

            
        if CURRENT_SECTION == MUSE_GRAPH:
            all_event_types = []
            for query_to_process in queries_to_process:
                for event_type in determine_all_primitive_events_of_projection(query_to_process):
                    all_event_types.append(event_type)

            for nw in network:
                for event_type in nw:
                    if event_type not in all_event_types:

                        nw.remove(event_type)

            current_highest = 0
            current_value = 0
            total_sum = 0
            for node_idx,nw in enumerate(network):
                for event_type in nw:
                    current_value += all_eventtype_output_rates[event_type]
                    total_sum += all_eventtype_output_rates[event_type]
                if current_value > current_highest:
                    current_highest = current_value
                    single_sink_evaluation_node = node_idx
                current_value = 0
                        
            query = Query_fragment("",[],[],"")
            if extract_muse_graph_queries(line) != None:
                query.query = extract_muse_graph_queries(line)

            if query.query in queries_to_process:
                print("single_sink_evaluation_node",single_sink_evaluation_node)
                single_sink_query_network.append(Query_fragment(query.query,determine_all_primitive_events_of_projection(query.query),[single_sink_evaluation_node],""))
            
            if extract_muse_graph_sources(line) != None:
                query.primitive_operators = extract_muse_graph_sub_queries(line)
                
            if extract_muse_graph_sources(line) != None:
                query.node_placement = extract_muse_graph_sources(line)

            if extract_muse_graph_forbidden(line) != None:
                query.forbidden_event_types = extract_muse_graph_forbidden(line)


            query_network.append(query)

        if CURRENT_SECTION == SELECTIVITIES:
            extract_muse_graph_selectivities(line)


    reversed_query_network = []
    for i in range(len(query_network)-2,-1,-1):
        reversed_query_network.append(query_network[i])
        if query_network[i].query == '':
            continue
        multi_sink_placement = is_single_sink_placement(query_network[i])
                
        eventtype_to_sources_map[query_network[i].query] = query_network[i].node_placement
        all_eventtype_output_rates[query_network[i].query] = determine_total_query_rate(query_network[i])
    for query in query_network:
        print(query.query)
    print("~~")
    query_network = reversed_query_network
    for query in single_sink_query_network:
        print(query.query)

    all_needed_primitive_events, biggest_query_length_to_be_processed = get_all_distinct_eventtypes_of_used_queries_and_largest_query()
    number_of_samples = int(runs)
    all_exact_costs = 0
    all_sampling_costs = 0
    all_factorial_costs = 0
    all_greedy_costs = 0

    all_exact_costs_single_sink = 0
    all_sampling_costs_single_sink = 0
    all_factorial_costs_single_sink = 0
    all_greedy_costs_single_sink = 0
    
    old_eventtype_pair_to_selectivity = eventtype_pair_to_selectivity.copy()
    
    query_network_copy = copy.deepcopy(query_network)
    single_sink_query_network_copy = copy.deepcopy(single_sink_query_network)
    all_costs = []
    
    highest_primitive_eventtype_to_be_processed = all_needed_primitive_events[len(all_needed_primitive_events)-1]
    all_eventtypes = [chr(i) for i in range(ord('A'),ord(highest_primitive_eventtype_to_be_processed)+1)]
    eventtype_combinations = determine_permutations_of_all_relevant_lengths(all_eventtypes, 2, biggest_query_length_to_be_processed)

    error_counter = 0

    greedy_accumulated_exec_time = 0
    greedy_worst_generated_costs = 0
    greedy_best_generated_costs = float('inf')

    exact_accumulated_exec_time = 0
    exact_worst_generated_costs = 0
    exact_best_generated_costs = float('inf')

    factorial_accumulated_exec_time = 0
    factorial_worst_generated_costs = 0
    factorial_best_generated_costs = float('inf')

    sampling_accumulated_exec_time = 0
    sampling_worst_generated_costs = 0
    sampling_best_generated_costs = float('inf')


    greedy_accumulated_exec_time_single_sink = 0
    greedy_worst_generated_costs_single_sink = 0
    greedy_best_generated_costs_single_sink = float('inf')

    exact_accumulated_exec_time_single_sink = 0
    exact_worst_generated_costs_single_sink = 0
    exact_best_generated_costs_single_sink = float('inf')

    factorial_accumulated_exec_time_single_sink = 0
    factorial_worst_generated_costs_single_sink = 0
    factorial_best_generated_costs_single_sink = float('inf')

    sampling_accumulated_exec_time_single_sink = 0
    sampling_worst_generated_costs_single_sink = 0
    sampling_best_generated_costs_single_sink = float('inf')
    central_push_costs = total_sum - current_highest
    print("Central push costs:", central_push_costs)
    for idx in range(1, number_of_samples+1):
        eventtype_pair_to_selectivity = old_eventtype_pair_to_selectivity.copy()
        eventtypes_single_selectivities = {}
        single_selectivity_of_eventtype_within_projection = {}
        determine_all_single_selectivities_for_every_possible_projection()

        q_network = query_network_copy if method == "ppmuse" else single_sink_query_network_copy

        if method == "ppmuse":
            greedy_costs, sampling_costs, factorial_costs, exact_costs, greedy_algo_time, exact_algo_time, factorial_algo_time, sampling_algo_time = determine_randomized_distribution_push_pull_costs(q_network, eventtype_combinations, highest_primitive_eventtype_to_be_processed, algorithm, samples, topk, plan_print)
            print("§§§§§§§§§§ Push-Pull MuSE costs §§§§§§§§§§")
            if algorithm == "g":
                print("####### GREEDY #######")
                print("Greedy network costs:", greedy_costs)
                all_greedy_costs += greedy_costs
                print("Greedy Average:", all_greedy_costs/idx)
                print("Greedy Average Transsmission Ratio:", (all_greedy_costs/idx) / central_push_costs)
                if greedy_costs < greedy_best_generated_costs:
                    greedy_best_generated_costs = greedy_costs
				
                if greedy_costs > greedy_worst_generated_costs:
                    greedy_worst_generated_costs = greedy_costs
				
                greedy_accumulated_exec_time += greedy_algo_time
                print("Average greedy algorithm execution time:", greedy_accumulated_exec_time/idx)
                

            if algorithm == "e":            
                print("####### EXACT #######")
                print("Exact network costs:", exact_costs)
                all_exact_costs += exact_costs
                print("Exact Average:", all_exact_costs/idx)
                print("Exact Average Transsmission Ratio:", (all_exact_costs/idx) / central_push_costs)
                if exact_costs < exact_best_generated_costs:
                    exact_best_generated_costs = exact_costs
				
                if exact_costs > exact_worst_generated_costs:
                    exact_worst_generated_costs = exact_costs
				
                exact_accumulated_exec_time += exact_algo_time
                print("Average exact algorithm execution time:", exact_accumulated_exec_time/idx)


            if algorithm == "f":  
                print("####### FACTORIAL #######")
                print("Factorial approximated network costs:", factorial_costs)
                all_factorial_costs += factorial_costs
                print("Factorial Average:", all_factorial_costs/idx)
                print("Factorial Average Transsmission Ratio:", (all_factorial_costs/idx) / central_push_costs)
                if factorial_costs < factorial_best_generated_costs:
                    factorial_best_generated_costs = factorial_costs
				
                if factorial_costs > factorial_worst_generated_costs:
                    factorial_worst_generated_costs = factorial_costs
				
                factorial_accumulated_exec_time += factorial_algo_time
                print("Average factorial approximation algorithm execution time:", factorial_accumulated_exec_time/idx)
                

            if algorithm == "s":
                print("####### SAMPLING #######")
                print("Sampling approximated network costs:", sampling_costs)
                all_sampling_costs += sampling_costs
                print("Sampling Average:", all_sampling_costs/idx)
                print("Sampling Average Transsmission Ratio:", (all_sampling_costs/idx) / central_push_costs)
                if sampling_costs < sampling_best_generated_costs:
                    sampling_best_generated_costs = sampling_costs
				
                if sampling_costs > sampling_worst_generated_costs:
                    sampling_worst_generated_costs = sampling_costs
			
                sampling_accumulated_exec_time += sampling_algo_time
                print("Average sampling approximation algorithm execution time:", sampling_accumulated_exec_time/idx)
                print("#######################")


        elif method == "cent":
            greedy_costs, sampling_costs, factorial_costs, exact_costs, greedy_algo_time, exact_algo_time, factorial_algo_time, sampling_algo_time = determine_randomized_distribution_push_pull_costs(q_network, eventtype_combinations, highest_primitive_eventtype_to_be_processed, algorithm, samples, topk, plan_print)
            print("§§§§§§§§§§ Centralized Push-Pull costs §§§§§§§§§§")
            if algorithm == "g":
                print("####### GREEDY #######")
                print("Greedy network costs:", greedy_costs)
                all_greedy_costs_single_sink += greedy_costs
                print("Greedy Average:", all_greedy_costs_single_sink/idx)
                print("Greedy Average Transsmission Ratio:", (all_greedy_costs_single_sink/idx) / central_push_costs)
                if greedy_costs < greedy_best_generated_costs_single_sink:
                    greedy_best_generated_costs_single_sink = greedy_costs
			
                if greedy_costs > greedy_worst_generated_costs_single_sink:
                    greedy_worst_generated_costs_single_sink = greedy_costs
				
                greedy_accumulated_exec_time_single_sink += greedy_algo_time
                print("Average greedy algorithm execution time:", greedy_accumulated_exec_time_single_sink/idx)


            if algorithm == "e":
                print("####### EXACT #######")
                print("Exact network costs:", exact_costs)
                all_exact_costs_single_sink += exact_costs
                print("Exact Average:", all_exact_costs_single_sink/idx)
                print("Exact Average Transsmission Ratio:", (all_exact_costs_single_sink/idx) / central_push_costs)
                if exact_costs < exact_best_generated_costs_single_sink:
                    exact_best_generated_costs_single_sink = exact_costs
				
                if exact_costs > exact_worst_generated_costs_single_sink:
                    exact_worst_generated_costs_single_sink = exact_costs
				
                exact_accumulated_exec_time_single_sink += exact_algo_time
                print("Average exact algorithm execution time:", exact_accumulated_exec_time_single_sink/idx)


            if algorithm == "f":
                print("####### FACTORIAL #######")
                print("Factorial approximated network costs:", factorial_costs)
                all_factorial_costs_single_sink += factorial_costs
                print("Factorial Average:", all_factorial_costs_single_sink/idx)
                print("Factorial Average Transsmission Ratio:", (all_factorial_costs_single_sink/idx) / central_push_costs)
                if factorial_costs < factorial_best_generated_costs_single_sink:
                    factorial_best_generated_costs_single_sink = factorial_costs
				
                if factorial_costs > factorial_worst_generated_costs_single_sink:
                    factorial_worst_generated_costs_single_sink = factorial_costs
				
                factorial_accumulated_exec_time_single_sink += factorial_algo_time
                print("Average factorial approximation algorithm execution time:", factorial_accumulated_exec_time_single_sink/idx)

            if algorithm == "s":
                print("####### SAMPLING #######")
                print("Sampling approximated network costs:", sampling_costs)
                all_sampling_costs_single_sink += sampling_costs
                print("Sampling Average:", all_sampling_costs_single_sink/idx)
                print("Sampling Average Transsmission Ratio:", (all_sampling_costs_single_sink/idx) / central_push_costs)
                if sampling_costs < sampling_best_generated_costs_single_sink:
                    sampling_best_generated_costs_single_sink = sampling_costs
				
                if sampling_costs > sampling_worst_generated_costs_single_sink:
                    sampling_worst_generated_costs_single_sink = sampling_costs
				
                sampling_accumulated_exec_time_single_sink += sampling_algo_time
                print("Average sampling approximation algorithm execution time:", sampling_accumulated_exec_time_single_sink/idx)
                print("#######################")

			
            print("Run:", idx)
        
            query_network_copy = copy.deepcopy(query_network)
            single_sink_query_network_copy = copy.deepcopy(single_sink_query_network)
            
