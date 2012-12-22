Feature: Check and Repair data tests
    Scenario: 8 nodes for 1Gb per node
        Given I have nodes with HDDs: 1, 1, 1, 1, 1, 1, 1, 1
        When start network 
        And start monitor
        And put data until HDDs have free capacity less than 25 
        Then wait check and repair operation finish with label: ALL valid data
        When destroy data from node with nums: 1
        Then wait check and repair operation finish with label: data lost from one node
        When destroy data from node with nums: 1, 3
        Then wait check and repair operation finish with label: data lost from 2 nodes
        When destroy data from node with nums: 1
        And check range from node with num: 1
        Then wait check and repair operation finish with label: data lost from one node (CHECK ONE RANGE ONLY)

    Scenario: 8 nodes for 1.5Gb per node
        Given I have nodes with HDDs: 1.5, 1.5, 1.5, 1.5,1.5, 1.5, 1.5, 1.5
        When start network 
        And start monitor
        And put data until HDDs have free capacity less than 25 
        Then wait check and repair operation finish with label: ALL valid data
        When destroy data from node with nums: 1
        Then wait check and repair operation finish with label: data lost from one node
        When destroy data from node with nums: 1, 3
        Then wait check and repair operation finish with label: data lost from 2 nodes
        When destroy data from node with nums: 1
        And check range from node with num: 1
        Then wait check and repair operation finish with label: data lost from one node (CHECK ONE RANGE ONLY)

    Scenario: 8 nodes for 2Gb per node
        Given I have nodes with HDDs: 2, 2, 2, 2, 2, 2, 2, 2
        When start network 
        And start monitor
        And put data until HDDs have free capacity less than 25 
        Then wait check and repair operation finish with label: ALL valid data
        When destroy data from node with nums: 1
        Then wait check and repair operation finish with label: data lost from one node
        When destroy data from node with nums: 1, 3
        Then wait check and repair operation finish with label: data lost from 2 nodes
        When destroy data from node with nums: 1
        And check range from node with num: 1
        Then wait check and repair operation finish with label: data lost from one node (CHECK ONE RANGE ONLY)

