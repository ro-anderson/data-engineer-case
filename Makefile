.PHONY: clean

SHELL=/bin/bash                                                                                                                                                                                

## Delete all .png, .csv, .txt and cache files
clean:
	find . -name "__pycache__" -type d -exec rm -r {} \+

