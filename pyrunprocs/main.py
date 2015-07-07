"""
    The mongoexup library.
    Structure of a task list:
    [
        {
         'op': operation1,
         'args': ['arg1', 'arg2', 'arg3' ... ],
         'result' : result,

         # Function that returns a boolean
         'pre_condition': {'func': func, 'args': [], 'kwargs':{func keyword args}},

         # Function that does processing and returns values to be used in arguments
         'pre_hook': {'func': func, 'args': [], 'kwargs':{func keyword args}},

         # Function that executes after the operation. The result of the operation is
         # available in the 'result' key and can be used here.
         'post_hook': {'func': func, 'args': [], 'kwargs':{func keyword args}},
        },
        {
         'op': operation2
         ...
        },
        {
         'op': operation3
         ...
        },
    ]
"""

import runprocs as eo
import types
import logging


def validate_task(task, logger):
    """ Validates the task tuple. """
    # Validate args for operations
    # For typed tasks, validate the args since we know what they should be.
    # If task is 'custom function', function should be present and
    # if function args are present, it should be tuple, list or dict.
    # If task is 'custom command', 'command args should be present
    # and should be a list.
    all_valid = []
    if not isinstance(task, dict):
        logger.debug("Task should be a dictionary")
        all_valid.append(False)
    else:
        all_valid.append(True)

    if 'op' not in task:
        logger.debug("Task should have an operation")
        all_valid.append(False)
    else:
        all_valid.append(True)

    if 'args' in task and not isinstance(task['args'], list):
        logger.debug("Args for operation {0} should be a list.".format(task['op']))
        all_valid.append(False)
    else:
        all_valid.append(True)
    all_valid.append(validate_functions(task, 'pre_condition', logger))
    all_valid.append(validate_functions(task, 'pre_hook', logger))
    all_valid.append(validate_functions(task, 'post_hook', logger))
    return all(all_valid)


def validate_functions(task, func_key, logger):
    if func_key in task and (not isinstance(task[func_key], (types.MethodType, types.FunctionType))):
        logger.debug("{0} for operation {1} should be a function".format(func_key, task['op']))
        return False
    return True


def run(task_list, logger=None, halt_on_error=True):
    """ Processes the task list. """
    if not logger:
        logging.basicConfig()
        logger = logging.getLogger(__name__)
    for task in task_list:
        validate_task(task, logger=logger)
    for task in task_list:
        eo.do_task(task, logger=logger, halt_on_error=halt_on_error)
