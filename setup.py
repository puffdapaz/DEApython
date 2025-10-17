from setuptools import setup, find_packages

setup(
    name="DEApython",
    version="0.1.0",
    author="Pedro Silva",
    description="Pipeline and DEA modeling using public data in Brazil",
    packages=find_packages(),
    install_requires=["basedosdados",
                      "dealib",
                      "dotenv",
                      "google-cloud-storage",
                      "numpy",
                      "pandas",
                      "pandera",
                      "pyarrow",
                      "PyYAML",
                      "requests",
                      "scipy",
                      "tomlkit",
                      "typing_extensions",
    ],
    python_requires=">=3.9",
)
