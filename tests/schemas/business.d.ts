interface _business {
  business_id: string;
  name: string;
  address: string;
  city: string;
  state: string;
  postal_code: string;
  latitude: number;
  longitude: number;
  stars: number;
  review_count: number;
  is_open: number;
  attributes: {
    RestaurantsTakeOut: string;
    RestaurantsReservations: string;
    RestaurantsAttire: string;
    OutdoorSeating: string;
    BestNights: {
      monday: boolean;
      tuesday: boolean;
      friday: boolean;
      wednesday: boolean;
      thursday: boolean;
      sunday: boolean;
      saturday: boolean;
    };
    RestaurantsTableService: string;
    Ambience: {
      touristy: boolean;
      hipster: boolean;
      romantic: boolean;
      divey: boolean;
      intimate: boolean;
      trendy: boolean;
      upscale: boolean;
      classy: boolean;
      casual: boolean;
    };
    RestaurantsPriceRange2: string;
    Alcohol: string;
    RestaurantsGoodForGroups: string;
    BikeParking: string;
    BusinessAcceptsCreditCards: string;
    GoodForKids: string;
    HappyHour: string;
    WiFi: string;
    CoatCheck: string;
    BusinessParking: {
      garage: boolean;
      street: boolean;
      validated: boolean;
      lot: boolean;
      valet: boolean;
    };
    GoodForMeal: {
      dessert: boolean;
      latenight: boolean;
      lunch: boolean;
      dinner: boolean;
      brunch: boolean;
      breakfast: boolean;
    };
    WheelchairAccessible: string;
    NoiseLevel: string;
    Caters: string;
    HasTV: string;
    Music: {
      dj: boolean;
      background_music: boolean;
      no_music: boolean;
      jukebox: boolean;
      live: boolean;
      video: boolean;
      karaoke: boolean;
    };
    Smoking: string;
    RestaurantsDelivery: string;
    GoodForDancing: string;
  };
  categories: string;
  hours: {
    Monday: string;
    Tuesday: string;
    Wednesday: string;
    Thursday: string;
    Friday: string;
    Saturday: string;
    Sunday: string;
  };
}
