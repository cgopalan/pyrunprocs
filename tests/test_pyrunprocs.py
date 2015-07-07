import unittest
import logging

import context
import main

logger = logging.getLogger()


class TestExup(unittest.TestCase):
    """ Tests for exup. """

    def test_op_invalid(self):
        task = {}
        self.assertFalse(main.validate_task(task, logger=logger))


    def test_op_valid(self):
        task = {'op': "myop"}
        self.assertTrue(main.validate_task(task, logger=logger))


    def test_args_invalid(self):
        task = {'op': "myop", 'args': "hello"}
        self.assertFalse(main.validate_task(task, logger=logger))


    def test_args_valid(self):
        task = {'op': "myop", 'args': ["hello"]}
        self.assertTrue(main.validate_task(task, logger=logger))


    def test_result_populated(self):
        task = {'op': "ls", 'args': ["-l"]}
        main.run([task], logger=logger)
        self.assertEquals(task['result'], 0)


    def test_precondition_false_skips_task(self):
        def dont_execute():
            return False
        task = {'op': "ls", 'args': ["-l"], 'pre_condition': {'func': dont_execute}}
        main.run([task], logger=logger)
        self.assertFalse('result' in task)


    def test_precondition_true_executes_task(self):
        def do_execute():
            return True
        task = {'op': "ls", 'args': ["-l"], 'pre_condition': {'func': do_execute}}
        main.run([task], logger=logger)
        self.assertTrue('result' in task)
        self.assertEquals(task['result'], 0)


    def test_prehook_func_substitutes_vars(self):
        def exec_before_task(a,b,c):
            return a,b,c
        task = {'op': "echo", 'args': ['hello', 'prehook.1', 'prehook.0', 'prehook.2'],
                'pre_hook': {'func': exec_before_task, 'args': ["how", "are", "ya"]}}
        main.run([task], logger=logger)
        self.assertEquals(task['args'], ["hello", "are", "how", "ya"])
        self.assertTrue('result' in task)
        self.assertEquals(task['result'], 0)


    def test_prehook_vars_do_not_match_func_return_values(self):
        def exec_before_task(a,b,c):
            return a,b,c
        task = {'op': "echo", 'args': ['prehook.0', 'prehook.1'],
                'pre_hook': {'func': exec_before_task, 'args': ["how", "are", "ya"]}}
        main.run([task], logger=logger)
        self.assertEquals(task['args'], ['prehook.0', 'prehook.1'])
        self.assertTrue('result' not in task)


    def test_posthook_func_executes_after_task(self):
        pass


if __name__ == "__main__":
    unittest.main()
