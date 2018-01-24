project('raftcpp', 'cpp', default_options : ['cpp_std=c++11'])

bmcl_mod = subproject('bmcl', default_options: ['build_tests=false', 'use_qt5=false'])
bmcl_dep = bmcl_mod.get_variable('bmcl_dep')

gtest_mod = subproject('gtest')
gtest_dep = gtest_mod.get_variable('gtest_dep')
gtest_main_dep = gtest_mod.get_variable('gtest_main_dep')

subdir('src')
subdir('tests')

#gtest_mod = subproject('gtest')
