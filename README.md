# AdvancedDatabaseMgmtMS
SELECT *
FROM detailed_store1_inventory
WHERE film_id = '388';
 
SELECT *
FROM summary_store1_inventory
WHERE film_id = '388';
 
SELECT *
FROM inventory
WHERE film_id = '388'; --Gunfight Moon
 
SELECT *
FROM rental
WHERE inventory_id = '1787'
OR inventory_id = '1788';
 
UPDATE inventory SET store_id = '2'
WHERE inventory_id = '1788';
 
 
CREATE VIEW currently_rented AS
SELECT inventory_id
FROM rental
WHERE return_date IS NULL;
 
CREATE VIEW store1_rented_films AS
SELECT film_id, count(film_id) as qty_rented
FROM inventory
INNER JOIN currently_rented ON inventory.inventory_id = currently_rented.inventory_id
WHERE store_id = '1'
GROUP BY film_id;
 
CREATE VIEW store1_inventory_stock AS
SELECT film.film_id, count(film.film_id) as total_qty
FROM inventory
INNER JOIN film on inventory.film_id = film.film_id
WHERE store_id = '1'
GROUP BY film.film_id;
 
CREATE TABLE detailed_store1_inventory AS
SELECT store1_inventory_statistics.*, film.title, film.rental_duration, film.rental_rate, film.replacement_cost
FROM (
  SELECT store1_inventory_stock.film_id, total_qty, qty_rented,
    COALESCE((total_qty - qty_rented), total_qty) as on_hand,
    ROUND(COALESCE(qty_rented::numeric/total_qty::numeric, 0), 4) as ratio_rented
  FROM store1_inventory_stock
  LEFT JOIN store1_rented_films ON store1_inventory_stock.film_id = store1_rented_films.film_id
  ORDER BY ratio_rented DESC, total_qty DESC) AS store1_inventory_statistics
JOIN film ON store1_inventory_statistics.film_id = film.film_id;
 
--Original SQL code for the summary table:
 
DROP TABLE summary_store1_inventory;
 
CREATE TABLE summary_store1_inventory AS
SELECT detailed_store1_inventory.film_id, detailed_store1_inventory.title, detailed_store1_inventory.total_qty, order_more(detailed_store1_inventory.ratio_rented) as need_more
FROM detailed_store1_inventory;
 
 
CREATE OR REPLACE FUNCTION order_more(this_ratio numeric)
RETURNS text AS
$$
BEGIN
	IF this_ratio >= 0.5 THEN
		RETURN 'Y';
	ELSE
		RETURN 'N';
	END IF;
END;
$$
LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION get_replacement_cost(this_id integer)
RETURNS numeric AS
$$
BEGIN
  RETURN (SELECT replacement_cost FROM film WHERE film_id = this_id);
END;
$$
LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION get_rental_rate(this_id integer)
RETURNS numeric AS
$$
BEGIN
  RETURN (SELECT rental_rate FROM film WHERE film_id = this_id);
END;
$$
LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION get_rental_duration(this_id integer)
RETURNS smallint AS
$$
BEGIN
  RETURN (SELECT rental_duration FROM film WHERE film_id = this_id);
END;
$$
LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION get_film_title(this_id integer)
RETURNS text AS
$$
BEGIN
  RETURN (SELECT title FROM film WHERE film_id = this_id);
END;
$$
LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION get_total_qty(this_id integer)
RETURNS bigint AS
$$
BEGIN
  RETURN (SELECT count(film_id) as total_qty
  FROM inventory
  WHERE this_id = inventory.film_id
    AND inventory.store_id = '1');
END;
$$
LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION get_rented_qty(this_id integer)
RETURNS bigint AS
$$
BEGIN
  RETURN (SELECT count(film_id) as qty_rented
    FROM inventory
    INNER JOIN (
      SELECT inventory_id
      FROM rental
      WHERE return_date IS NULL) AS currently_rented 
    ON inventory.inventory_id = currently_rented.inventory_id
    WHERE store_id = '1'
    AND film_id = this_id
    GROUP BY film_id);
END;
$$
LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION does_value_exist(this_id integer, this_table text, this_column text)
RETURNS boolean AS
$$
DECLARE
  found boolean := false;
BEGIN
  EXECUTE 'SELECT EXISTS (SELECT 1 FROM' || this_table || ' WHERE ' || this_column || ' = $1)' INTO found USING this_id;
  RETURN found;
END;
$$
LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION update_detailed_store1_inventory()
RETURNS TRIGGER AS
$$
DECLARE
  new_total_qty integer := get_total_qty(NEW.film_id);
  new_rented_qty integer := get_rented_qty(NEW.film_id);
  new_ratio_rented numeric := ROUND(COALESCE(new_rented_qty::numeric/new_total_qty::numeric, 0), 4);
BEGIN
  IF NEW.store_id = '1' THEN
    IF TG_OP = 'INSERT' THEN
      IF does_value_exist(NEW.film_id, 'inventory', 'film_id') THEN
        UPDATE detailed_store1_inventory
        SET total_qty = new_total_qty, qty_rented = new_rented_qty, on_hand = (new_total_qty - new_rented_qty), ratio_rented = new_ratio_rented, title = get_film_title(NEW.film_id), rental_duration = get_rental_duration(NEW.film_id), rental_rate = get_rental_rate(NEW.film_id), replacement_cost = get_replacement_cost(NEW.film_id)
        WHERE film_id = NEW.film_id;
      ELSE
        INSERT INTO detailed_store1_inventory (film_id, total_qty, qty_rented, on_hand, ratio_rented, title, rental_duration, rental_rate, replacement_cost)
        VALUES (NEW.film_id, new_total_qty, new_rented_qty, (new_total_qty - new_rented_qty), new_ratio_rented, get_film_title(NEW.film_id), get_rental_duration(NEW.film_id), get_rental_rate(NEW.film_id), get_replacement_cost(NEW.film_id));
      END IF;
    ELSIF TG_OP = 'DELETE' and new_total_qty < 1 AND new_rented_qty < 1 THEN
      DELETE FROM detailed_store1_inventory
      WHERE detailed_store1_inventory.film_id = OLD.film_id;
    ELSIF TG_OP = 'DELETE' OR TG_OP = 'UPDATE' THEN
      UPDATE detailed_store1_inventory
      SET total_qty = new_total_qty, qty_rented = new_rented_qty, on_hand = (new_total_qty - new_rented_qty), ratio_rented = new_ratio_rented, title = get_film_title(NEW.film_id), rental_duration = get_rental_duration(NEW.film_id), rental_rate = get_rental_rate(NEW.film_id), replacement_cost = get_replacement_cost(NEW.film_id)
      WHERE film_id = NEW.film_id;
    END IF;
  ELSIF NOT NEW.store_id = '1' AND OLD.store_id = '1' AND TG_OP = 'UPDATE' THEN
    UPDATE detailed_store1_inventory
    SET total_qty = new_total_qty, qty_rented = new_rented_qty, on_hand = (new_total_qty - new_rented_qty), ratio_rented = new_ratio_rented, title = get_film_title(NEW.film_id), rental_duration = get_rental_duration(NEW.film_id), rental_rate = get_rental_rate(NEW.film_id), replacement_cost = get_replacement_cost(NEW.film_id)
    WHERE film_id = NEW.film_id;
  END IF;
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;
 
CREATE OR REPLACE FUNCTION update_summary_store1_inventory()
RETURNS TRIGGER AS
$$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO summary_store1_inventory (film_id, title, total_qty, need_more)
      VALUES (NEW.film_id, NEW.title, NEW.total_qty, order_more(NEW.ratio_rented));
  ELSIF TG_OP = 'UPDATE' THEN
    UPDATE summary_store1_inventory
    SET title = NEW.title, total_qty = NEW.total_qty, need_more = order_more(NEW.ratio_rented)
    WHERE film_id = NEW.film_id;
  ELSIF TG_OP = 'DELETE' THEN
    DELETE FROM summary_store1_inventory
    WHERE summary_store1_inventory.film_id = OLD.film_id;
  END IF;
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;
 
CREATE TRIGGER on_update_inventory
AFTER INSERT OR DELETE OR UPDATE ON inventory
FOR EACH ROW
EXECUTE FUNCTION update_detailed_store1_inventory();
 
CREATE TRIGGER on_update_detailed_report
AFTER INSERT OR DELETE OR UPDATE ON detailed_store1_inventory
FOR EACH ROW
EXECUTE FUNCTION update_summary_store1_inventory();
