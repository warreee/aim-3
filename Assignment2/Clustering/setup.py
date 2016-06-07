from setuptools import setup


def readme():
    with open('README.md') as f:
        return f.read()

setup(name='Pixel extractor',
      version='0.1',
      description='Project to transform images to csv pixel data.',
      long_description=readme(),
      url='https://github.com/JonathanH5/',
      author='Jonathan Hasenburg',
      author_email='Jonathan@Hasenburg.de',
      license='MIT',
      packages=['image_clustering'],
      install_requires=[
            'Pillow'
      ],
      zip_safe=False)
