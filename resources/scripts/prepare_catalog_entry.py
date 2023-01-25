from typing import Optional
from datetime import datetime

from pydantic import BaseModel, AnyHttpUrl, FileUrl, validator, root_validator
import yaml

STEPS = 11


class Contact(BaseModel):
    name: Optional[str]
    email: Optional[str]
    webpage: Optional[str] = None  # AnyHttpUrl

    @root_validator(pre=True)
    def match_contact(cls, values):
        print(f"Step 8/{STEPS}: Defining contact person")
        return values

    @validator("name", pre=True, always=True)
    def match_name(cls, _, values):
        return input(f"Step 8.1: Name of the contact person: ")

    @validator("email", pre=True, always=True)
    def match_email(cls, _, values):
        return input(f"Step 8.2: Email of the contact person: ")

    @validator("webpage", pre=True, always=True)
    def match_webpage(cls, _, values):
        if url := input(f"Step 8.3: Webpage (optional): ").strip() == "":
            return None
        return url


class License(BaseModel):
    name: Optional[str]
    webpage: Optional[str]  # AnyHttpUrl

    @root_validator(pre=True)
    def match_license(cls, values):
        print(f"Step 9/{STEPS}: Defining license")
        return values

    @validator("name", pre=True, always=True)
    def match_name(cls, _, values):
        return input(f"Step 9.1: Name of the license: ")

    @validator("webpage", pre=True, always=True)
    def match_webpage(cls, _, values):
        return input(f"Step 9.2: Webpage of the license: ")


class RelatedData(BaseModel):
    name: Optional[str]
    webpage: Optional[str]  # AnyHttpUrl

    @validator("name", pre=True, always=True)
    def match_name(cls, _, values):
        return input(f"Step 10.1: Name of the related data: ")

    @validator("webpage", pre=True, always=True)
    def match_webpage(cls, _, values):
        return input(f"Step 10.2: Webpage of the related data: ")


class Metadata(BaseModel):
    dataset_id: Optional[str]
    description: Optional[str]
    attribution: Optional[str]
    label: Optional[str]
    image: Optional[str]  # FileUrl
    doi: Optional[str]
    publication_date: Optional[str]
    contact: Optional[Contact]
    license: Optional[License]
    related_data: Optional[list[RelatedData]]

    @validator("dataset_id", pre=True, always=True)
    def match_dataset_id(cls, _, values):
        while True:
            dataset_id = input(
                f"Step 1/{STEPS}: What is the name of the dataset (no"
                " whitspaces)? "
            )
            for letter in dataset_id:
                if letter.isspace():
                    print("Dataset id cannot have whitespaces")
                    break
            else:
                if dataset_id.strip() != "":
                    return dataset_id

    @validator("description", pre=True, always=True)
    def match_desc(cls, _, values):
        return input(f"Step 2/{STEPS}: Dataset description: ")

    @validator("attribution", pre=True, always=True)
    def match_attr(cls, _, values):
        return input(f"Step 3/{STEPS}: Dataset attribution: ")

    @validator("label", pre=True, always=True)
    def match_label(cls, _, values):
        while True:
            lab = input(f"Step 4/{STEPS}: Dataset label [<100 characters]: ")
            if len(lab) >= 100:
                print(
                    f"Label of size {len(lab)} is too long. It should be <100"
                )
                continue
            return lab

    @validator("image", pre=True, always=True)
    def match_img(cls, _, values):
        return input(f"Step 5/{STEPS}: Dataset image URL: ")

    @validator("doi", pre=True, always=True)
    def match_doi(cls, _, values):
        if (
            doi := input(f"Step 6/{STEPS}: Dataset DOI (optional): ").strip()
        ) == "":
            return None
        return doi

    @validator("publication_date", pre=True, always=True)
    def match_pub_date(cls, _, values):
        while True:
            pub_date = input(
                f"Step 7/{STEPS}: Publication date (YYYY-MM-DD): "
            )
            try:
                return datetime.strptime(pub_date, "%Y-%m-%d").strftime(
                    "%Y-%m-%d"
                )
            except ValueError as err:
                print(err)

    @validator("contact", pre=True, always=True)
    def match_contact(cls, _, values):
        return Contact()

    @validator("license", pre=True, always=True)
    def match_license(cls, _, values):
        return License()

    @validator("related_data", pre=True, always=True)
    def match_rel_data(cls, _, values):
        print(f"Step 10/{STEPS}: Defining related data")
        while True:
            rel_data_nbr = input(
                f"Step 10.0/{STEPS}: How many related data would you like to"
                " define? "
            )
            rel_data = []
            try:
                rel_data_nbr = int(rel_data_nbr)
            except ValueError:
                print("You should pass a number!")
            else:
                break
        for i in range(1, rel_data_nbr + 1):
            breakpoint()
            print(f"Related dataset {i}/{rel_data_nbr}")
            rel_data.append(RelatedData())
        return rel_data


class XarrayKwrgs(BaseModel):
    parallel: Optional[bool] = True
    decode_coords: Optional[str] = "all"
    chunks: Optional[dict] = None


class Args(BaseModel):
    path: Optional[str]
    delay_read_cubes: Optional[bool] = False
    field_id: Optional[str] = None
    metadata_caching: Optional[bool] = True
    metadata_cache_path: Optional[str] = None
    chunks: Optional[dict]
    mapping: Optional[dict]
    xarray_kwargs: Optional[XarrayKwrgs] = None

    @root_validator(pre=True)
    def match_root(cls, values):
        print(
            f"Step 11.6: Defining arguments !! Leave default if you don't"
            f" know !! "
        )
        return values

    @validator("path", pre=True, always=True)
    def match_path(cls, _, values):
        while True:
            path = input(
                f"Step 11.6.1: Path (use glob patterns if required!) "
            )
            if path.strip() != "":
                return path

    @validator("xarray_kwargs", pre=True, always=True)
    def match_xarray_kwargs(cls, _, values):
        return


class ProdMetadata(BaseModel):
    role: Optional[str]

    @validator("role", pre=True, always=True)
    def match_role(cls, _, values):
        role = (
            input(f"Step 11.3: Product role (optional) [default: public]: ")
            .lower()
            .strip()
        )
        if role == "":
            return "public"
        return role


class Product(BaseModel):
    product_id: Optional[str]
    description: Optional[str]
    metadata: Optional[ProdMetadata]
    maximum_query_size_gb: Optional[float] = 10
    driver: Optional[str] = "geokube_netcdf"
    args: Optional[Args]

    @validator("product_id", pre=True, always=True)
    def match_product_id(cls, _, values):
        while True:
            dataset_id = input(
                f"Step 11.1: What is the name of the product (no whitspaces)? "
            )
            if dataset_id.strip() != "":
                return dataset_id

    @validator("description", pre=True, always=True)
    def match_desc(cls, _, values):
        return input(f"Step 11.2: Product description: ")

    @validator("metadata", pre=True, always=True)
    def match_role(cls, _, values):
        print("Step 11.3: Product metadata")
        return ProdMetadata()

    @validator("maximum_query_size_gb", pre=True, always=True)
    def match_query_limit(cls, _, values):
        query_limit = (
            input(
                f"Step 11.4: Maximum query size in GB (optional) [default: 10"
                f" GB]: "
            )
            .lower()
            .strip()
        )
        if query_limit == "":
            return 10
        try:
            query_limit = float(query_limit)
        except ValueError:
            print("Query limit should be a number!")
        else:
            return query_limit

    @validator("driver", pre=True, always=True)
    def match_driver(cls, _, values):
        driver = (
            input(
                f"Step 11.5: Driver to use. !! Leave default if you don't"
                f" know!! [default: geokube_netcdf]: "
            )
            .lower()
            .strip()
        )
        if driver == "":
            return "geokube_netcdf"
        return driver

    @validator("args", pre=True, always=True)
    def match_args(cls, _, values):
        return Args()


class CatalogEntry(BaseModel):
    metadata: Optional[Metadata]
    sources: Optional[dict[str, Product]]

    @validator("metadata", pre=True, always=True)
    def match_metadata(cls, _, values):
        return Metadata()

    @validator("sources", pre=True, always=True)
    def match_sources(cls, _, values):
        print(f"Step 11/{STEPS}: Defining products")
        while True:
            prod_nbr = input(
                f"Step 11.0: How many products would you like to define? "
            )
            try:
                prod_nbr = int(prod_nbr)
            except ValueError:
                print("You should pass a number!")
            else:
                break
        prod_data = {}
        for i in range(1, prod_nbr + 1):
            print(f"Product {i}/{prod_nbr}")
            prod = Product()
            prod_data[prod.product_id] = prod
        return prod_data


if __name__ == "__main__":
    print("=== Preparing the new catalog entry .yaml file! ===")
    entry = CatalogEntry()
    file_name = f"{entry.metadata.dataset_id}.yaml"
    with open(file_name, "wt") as file:
        yaml.dump(entry.dict(), file)

    print(
        f"The catalog entry file '{file_name}' was generated! Now send it to:"
        " dds-support@cmcc.it"
    )
