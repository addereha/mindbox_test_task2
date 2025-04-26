from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

def get_product_category_pairs(
    products: DataFrame,
    categories: DataFrame,
    associations: DataFrame
) -> DataFrame:
    """
    Возвращает DataFrame со всеми парами (product_name, category_name),
    а также строки с product_name и NULL в category_name для продуктов без категорий.

    :param products: DataFrame с колонками (product_id, product_name, ...)
    :param categories: DataFrame с колонками (category_id, category_name, ...)
    :param associations: DataFrame с колонками (product_id, category_id, ...)
    :return: DataFrame с колонками (product_name, category_name)
    """
    # 1) Все связки product ↔ category
    product_with_cat = (
        associations
        # связываем с продуктами
        .join(products.select("product_id", "product_name"), on="product_id", how="inner")
        # связываем с категориями
        .join(categories.select("category_id", "category_name"), on="category_id", how="inner")
        .select("product_name", "category_name")
    )

    # 2) Продукты без категорий
    products_no_cat = (
        products
        # выбираем те продукты, у которых нет ни одной записи в associations
        .join(associations.select("product_id").distinct(), on="product_id", how="left_anti")
        .select(
            products["product_name"],
            # у таких продуктов ставим NULL в колонке category_name
            lit(None).alias("category_name")
        )
    )

    # 3) Объединяем обе выборки в один DataFrame
    result = product_with_cat.unionByName(products_no_cat)

    return result