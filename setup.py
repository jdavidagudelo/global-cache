from setuptools import setup

setup(name='ubidots_global_cache',
      version='0.4',
      description='The Ubidots Global Cache',
      url='https://github.com/jdavidagudelo/global-cache.git',
      author='jdavidagudelo',
      author_email='jdaaa2009@gmail.com',
      license='MIT',
      packages=['global_cache'],
      zip_safe=False, install_requires=['redis'])
