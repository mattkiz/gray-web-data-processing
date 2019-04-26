import csv
import json
import logging
import re
import os

from io import BytesIO, StringIO

from lxml import etree


class CountingFeatureExtractor:
    def __init__(self, config_file_path=None, meta_features=[]):
        self._extracted_feature_criteria = {}
        self._extracted_content_criteria = {}
        self._meta_features = meta_features
        self.feature_counts = []

        if config_file_path is not None:
            self.load_extracted_features(config_file_path)

    def load_extracted_features(self, config_file_path):
        load_feature_criteria(
            config_file_path,
            self._extracted_feature_criteria,
            self._extracted_content_criteria
        )

    def add_meta_feature(self, name):
        if name not in self._meta_features:
            self._meta_features.append(name)

    def add_extracted_content(self, name, xpath):
        self._validate_new_feature(name, xpath)
        put_feature_criterion(self._extracted_feature_criteria, name, xpath)

    def add_extracted_feature(self, name, xpath, text_re_mode, text_re_pattern):
        self._validate_new_feature(name, xpath, text_re_mode, text_re_pattern)
        put_feature_criterion(self._extracted_feature_criteria, name, xpath, text_re_mode, text_re_pattern)

    def _validate_new_feature(self, name, xpath, text_re_mode=None, text_re_pattern=None):
        if name in self._extracted_feature_criteria or self._extracted_content_criteria:
            raise ValueError("There is already a feature named '%s'." % name)
        if xpath is None:
            raise ValueError("An XPath expression is required for criteria.")
        if (text_re_mode is None and text_re_pattern is not None) or (
                text_re_mode is not None and text_re_pattern is None):
            raise ValueError("text_re_mode and text_re_pattern must both be defined or None")
        if text_re_mode is not None and text_re_mode not in ["match", "search"]:
            raise ValueError("text_re_mode should be None, 'match', or 'search'")

    def all_feature_names(self):
        feature_names = []
        for meta_feature_name in self._meta_features:
            feature_names.append(meta_feature_name)
        for extracted_feature_name in self._extracted_feature_criteria:
            feature_names.append(extracted_feature_name)
        for extracted_content_name in self._extracted_content_criteria:
            feature_names.append(extracted_content_name)
        return feature_names

    def accumulate_features_from_string(self, text, meta_features={}):
        self._append_features(
            extract_features_from_string(
                self._extracted_feature_criteria,
                self._extracted_content_criteria,
                text
            ),
            meta_features
        )
        return self.feature_counts

    def accumulate_features_from_bytes(self, byte_string, meta_features={}):
        self._append_features(
            extract_features_from_bytes(
                self._extracted_feature_criteria,
                self._extracted_content_criteria,
                byte_string
            ),
            meta_features
        )


    def accumulate_features_from_file(self, source_file, meta_features={}):
        self._append_features(
            extract_features_from_file(
                self._extracted_feature_criteria,
                self._extracted_content_criteria,
                source_file
            ),
            meta_features
        )

    def _append_features(self, extracted_features, meta_features):
        for meta_feature_name in meta_features:
            extracted_features[meta_feature_name] = meta_features[meta_feature_name]
        self.feature_counts.append(extracted_features)


def extract_features_from_string(feature_criteria, content_criteria, text):
    """
    Extract features from an ASCII string.
    Returns a dictionary of the number of matches for each criteria. The keys of the
    dictionary are the names of the criteria.
    """
    return extract_features_from_file(
        feature_criteria,
        content_criteria,
        StringIO(text)
    )


def extract_features_from_bytes(feature_criteria, content_criteria, bytes_string):
    """
    Extract features from a byte string.
    Returns a dictionary of the number of matches for each criteria. The keys of the
    dictionary are the names of the criteria.
    """
    return extract_features_from_file(
        feature_criteria,
        content_criteria,
        BytesIO(bytes_string)
    )


def extract_features_from_file(feature_criteria, content_criteria, source_file):
    """
    Extract features from a file like object.
    Returns a dictionary of the number of matches for each criteria. The keys of the
    dictionary are the names of the criteria.
    """

    # Parse the HTML
    parser = etree.HTMLParser(recover=True)
    return extract_features_from_tree(
        feature_criteria,
        content_criteria,
        etree.parse(source_file, parser=parser)
    )


def extract_features_from_tree(feature_criteria, content_criteria, html_tree):
    # Store the number of matches in the file for each criteria's XPath query
    data = {}
    for feature_name in feature_criteria:
        feature = feature_criteria[feature_name]
        elements = html_tree.xpath(feature["xpath"])
        if "text_re_mode" in feature and "text_re_pattern" in feature:
            if feature["text_re_mode"] == "match":
                pattern_matcher = re.match
            elif feature["text_re_mode"] == "search":
                pattern_matcher = re.search
            else:
                raise ValueError(
                    "Incorrect 'text_re_mode' %s for feature %s. Expected either 'match' or 'search'" % feature[
                        "text_re_mode"], feature_name)

            data[feature_name] = 0
            for element in elements:
                if pattern_matcher(feature["text_re_pattern"], _custom_str(element.text)):
                    data[feature_name] += 1
        else:
            data[feature_name] = len(elements)

    for feature_name in content_criteria:
        feature = content_criteria[feature_name]
        data[feature_name] = ""
        for element in html_tree.xpath(feature["xpath"]):
            data[feature_name] += _custom_str(element.text)

    return data


def extract_features_from_directory(feature_criteria, directory_path):
    """
    Extract features from all HTML files in a directory
    Returns a list of dictionaries. Each dicationary contains the number of matches for each
    criteria. The keys of the dictionaries are the names of the criteria.
    NOTE: The keys "path" and "file" are dynamically added to help track the sources
    """
    data_rows = []

    for file_name in os.listdir(directory_path):
        # Only extract data from HTML files
        if file_name.endswith(".html"):
            with open(os.path.join(directory_path, file_name), "r") as f:
                data = extract_features_from_file(feature_criteria, f)
                data["path"] = directory_path
                data["file"] = file_name
                data_rows.append(data)

    return data_rows


def load_feature_criteria(config_file_path, feature_criteria={}, extracted_content={}):
    """
    Loads criteria from the JSON file described in the path and returns the dictionary.
    The criteria define the features to search for.
    """
    with open(config_file_path, "r") as f:
        rubric = json.load(f)
        for feature in rubric["features_to_count"]:
            put_feature_criterion(
                feature_criteria,
                feature["name"],
                feature["xpath"],
                feature["text_re_mode"] if "text_re_mode" in feature else None,
                feature["text_re_pattern"] if "text_re_pattern" in feature else None
            )

        for feature in rubric["text_to_extract"]:
            put_feature_criterion(
                extracted_content,
                feature["name"],
                feature["xpath"]
            )

    return feature_criteria


def put_feature_criterion(feature_criteria, name, xpath, re_mode=None, re_pattern=None):
    feature_criteria[name] = {
        "xpath": xpath
    }
    if re_mode is not None:
        feature_criteria[name]["text_re_mode"] = re_mode
    if re_pattern is not None:
        feature_criteria[name]["text_re_pattern"] = re_pattern


def _custom_str(value):
    """
    Serializes the given value using the str method if the value is not None.
    If the value is None then the empty string is returned
    """
    if value is None:
        return ""
    if isinstance(value, str) or isinstance(value, bytes):
        return value
    return str(value)


if __name__ == "__main__":
    extractor = CountingFeatureExtractor(
        "./config/features.json",
        [
            "requested_url",
            "response_url"
        ]
    )

    with open("./out.csv", "w+") as o:
        csv_out = csv.DictWriter(o, extractor.all_feature_names())
        csv_out.writeheader()


        def get_requested_url(html_directory):
            with open(os.path.join(html_directory, "request_meta.json")) as f:
                return json.load(f)["http"]["url"]


        def get_response_url(html_direcotry):
            with open(os.path.join(html_direcotry, "response_meta.json")) as f:
                return json.load(f)["http"]["response_url"]


        def directory_walker(current_directory):
            data = []
            for file_name in os.listdir(current_directory):
                file_path = os.path.join(current_directory, file_name)
                if file_name.endswith(".html"):
                    data.append({
                        "requested_url": get_requested_url(current_directory),
                        "response_url": get_response_url(current_directory),
                        "file_path": file_path
                    })
                elif os.path.isdir(file_path):
                    data += directory_walker(file_path)

            return data


        directory_path = "./data"
        data = directory_walker(directory_path)
        skipped = 0
        for page_data in data:
            # Only extract data from HTML files
            try:
                extractor.accumulate_features_from_file(
                    page_data["file_path"],
                    {
                        "requested_url": page_data["requested_url"],
                        "response_url": page_data["response_url"]
                    }
                )
            except etree.XMLSyntaxError:
                skipped += 1
                print("Cannot parse %s due to syntax error" % page_data["requested_url"])

        print("Skipped %d of %d" % (skipped, len(data)))

        csv_out.writerows(extractor.feature_counts)