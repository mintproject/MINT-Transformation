
SILENT = True
if not(SILENT):
    print('Importing TopoFlow 3.6 packages:')
    print('   topoflow.utils')
    print('   topoflow.utils.tests')
    print('   topoflow.components')
    print('   topoflow.components.tests')
    print('   topoflow.framework')
    print('   topoflow.framework.tests')
    # print '   topoflow.gui (unfinished)'
    print(' ')

import funcs.topoflow.topoflow.utils
import funcs.topoflow.topoflow.utils.tests
#-----------------------------------
import funcs.topoflow.topoflow.components
import funcs.topoflow.topoflow.components.tests
#-----------------------------------
import funcs.topoflow.topoflow.framework
import funcs.topoflow.topoflow.framework.tests

#------------------------------------------
# This imports EMELI and prints more info
#------------------------------------------
import funcs.topoflow.topoflow.framework.tests.test_framework

#--------------------------------------
# The Python GUI is not yet finished.
#--------------------------------------
# import topoflow.gui

#----------------------------------------------
# Idea:  Import the topoflow_test() function,
#        so it can be executed with:
#    >>> import topoflow
#    >>> topoflow.topoflow_test().
#----------------------------------------------
# This doesn't work.
# from topoflow.framework.tests.test_framework import topoflow_test

