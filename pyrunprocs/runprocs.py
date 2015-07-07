"""
    The mongo exup operations implementation.
"""

import subprocess
import sys


def execute_func(task, func_type):
    func = task[func_type]['func']
    if ('args' in task[func_type] and
                'kwargs' in task[func_type]):
        cond = func(*task[func_type]['args'], **task[func_type]['kwargs'])
    elif 'args' in task[func_type]:
        cond = func(*task[func_type]['args'])
    elif 'kwargs' in task[func_type]:
        cond = func(**[func_type]['kwargs'])
    else:
        cond = func()
    return cond


def do_task(task, logger, halt_on_error=True):
    """ Do the task. """

    # Check if the task should be executed at all.
    if 'pre_condition' in task:
        try:
            cond = execute_func(task, 'pre_condition')
            if not cond:
                logger.debug("Skipped Task {0}".format(task['op']))
                return
        except Exception as exc:
            logger.debug("Exception thrown: {0}".format(exc))
            if halt_on_error:
                logger.debug("Halt On Error is True for task {0}. Exiting.".format(task['op']))
                sys.exit(0)
            else:
                return

    # Run pre-hook function if it exists.
    if 'pre_hook' in task:
        try:
            return_values = execute_func(task, 'pre_hook')
        except Exception as exc:
            logger.debug("Exception thrown: {0}".format(exc))
            if halt_on_error:
                logger.debug("Halt On Error is True for task {0}. Exiting.".format(task['op']))
                sys.exit(0)
            else:
                return
        else:
            # Check if the prehook placeholders are set.
            prehook_args = [(index, int(arg.split('.')[1])) for index,arg in enumerate(task['args']) if arg.startswith('prehook.')]
            if isinstance(return_values, (list, tuple)):
                if len(prehook_args) != len(return_values):
                    logger.debug("Prehook function doesnt return the same number of values as there are placeholders")
                    return
            else:
                if len(prehook_args) != 1:
                    logger.debug("Prehook function doesnt return the same number of values as there are placeholders")
                    return
            # Substitute placeholders with prehook function return values
            for arg_index, ret_val_index in prehook_args:
                task['args'][arg_index] = return_values[ret_val_index]

    # This task can execute.
    popen_args = [task['op']]
    popen_args.extend(task['args'])
    logger.debug("Starting task {0} with arguments {1}".format(task['op'],task['args']))
    task['result'] = run_process(popen_args, logger=logger)

    # Run post-hook function if it exists.
    if 'post_hook' in task:
        try:
            return_values = execute_func(task, 'post_hook')
        except Exception as exc:
            logger.debug("Exception thrown: {0}".format(exc))
            if halt_on_error:
                logger.debug("Halt On Error is True for task {0}. Exiting.".format(task['op']))
                sys.exit(0)
            else:
                return

def run_process(popen_args, logger=None):
    """ This function is passed an array of popen arguments, the name of the
        process being run and the type (export/upload) and this function will
        run the process, check for failure, and insert information into the DB. """
    popen_obj_returncode = 99
    popen_obj_out = ["", ""]

    try:
        logger.debug("About to do the subprocess with the "
                          "following args: {0}".format(popen_args))
        popen_obj = subprocess.Popen(popen_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        logger.debug(logger, "Waiting for the process to end")
        popen_obj.wait()
        popen_obj_returncode = popen_obj.returncode
        logger.debug("Return code is {0}".format(popen_obj_returncode))

        popen_obj_out = popen_obj.communicate()
        logger.debug("Communicate returns {0}".format(popen_obj_out))
    except Exception as e:
        # Record an exception into the DB and return nonzero
        logger.debug("Caught an exception : {0}".format(e))
        return 1

    if popen_obj_returncode != 0:
        logger.debug("popen's return code is non-zero")
        return 1

    return 0
