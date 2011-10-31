from distutils.core import setup
import sys

if sys.version<"2.3":
    setup(name="BplusPy",
      version="0.1",
      description="BplusDotNet python implementation",
      author="Aaron Watters",
      author_email="aaron_watters@sourceforge.net",
      url="http://bplusdotnet.sourceforge.net/",
      packages=['BplusPy'],
     )
else:
    setup(name="BplusPy",
      version="1.0",
      description="BplusDotNet python implementation",
      author="Aaron Watters",
      author_email="aaron_watters@sourceforge.net",
      url="http://bplusdotnet.sourceforge.net/",
      packages=['BplusPy'],

        classifiers = [
          'Intended Audience :: Science/Research',
          'Intended Audience :: End Users/Desktop',
          'Intended Audience :: Information Technology',
          'Intended Audience :: Developers',
          'Intended Audience :: System Administrators',
          'License :: OSI Approved :: Python Software Foundation License',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: Microsoft :: Windows',
          'Operating System :: POSIX',
          'Operating System :: OS Independent',
          'Programming Language :: Python',
          'Topic :: Database :: Database Engines/Servers',
          ],
     )
