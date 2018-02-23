
from setuptools import setup, find_packages
import os

version = '0.1.0.alpha'

setup(name='grpc-resolver',
      version=version,
      description=(
          "A simple Python gRPC service resolver and registry based on Etcd."),
      long_description=open(
          os.path.join(os.path.dirname(__file__), 'README.rst')).read(),
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'Programming Language :: Python',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Topic :: Internet :: WWW/HTTP :: HTTP Servers',
          'Topic :: Software Development :: Version Control :: Git',
          'Topic :: System :: Networking',
      ],
      keywords='gRPC Etcd service resolver registry',
      author='SenbinYu',
      author_email='justdoit920823@gmail.com',
      url='https://github.com/justdoit0823/grpc-resolver',
      license='Apache2.0',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      setup_requires=('pytest-runner',),
      install_requires=(
          # -*- Extra requirements: -*-
          'grpcio>=1.3.0,<=1.8.0',
          'etcd3>=0.6.2',
          'six>=1.10.0'
      ),
      tests_require=(
          'pytest',
          'pytest-runner',
          'pytest-mock>=1.6.0'
      ),
      entry_points="""
      # -*- Entry points: -*-
      """
)
