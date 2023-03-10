import setuptools

with open("README.md", "r") as fh:
  long_description = fh.read()

setuptools.setup(
  name="wxDownload-Shawn",
  version="1.0.0",
  author="Shawn Wang",
  author_email="940160835@qq.com",
  description="A multi-thread download tool",
  long_description=long_description,
  long_description_content_type="text/markdown",
  url="https://github.com/leo421/wxDownload",
  packages=setuptools.find_packages(),
  classifiers=[
    "Programming Language :: Python :: 3",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
  ],
)